package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
)

type SyncerConfig struct {
	VaultAddr  string
	VaultToken string
	MountPath  string
	VaultPath  string
	LocalPath  string
	CasTry     int
}

type Syncer struct {
	SyncerConfig
}

func NewSyncer(config SyncerConfig) *Syncer {
	config.VaultPath = strings.TrimPrefix(path.Clean(config.VaultPath), "/")
	config.LocalPath = strings.TrimPrefix(path.Clean(config.LocalPath), "/")
	return &Syncer{
		SyncerConfig: config,
	}
}

func (s *Syncer) Sync(ctx context.Context) error {
	client, err := vault.New(
		vault.WithAddress(s.VaultAddr),
		vault.WithRequestTimeout(30*time.Second),
	)
	if err != nil {
		return fmt.Errorf("failed to create vault client: %w", err)
	}

	err = client.SetToken(s.VaultToken)
	if err != nil {
		return fmt.Errorf("failed to set vault token: %w", err)
	}

	// set or update kv
	err = filepath.WalkDir(s.LocalPath, func(filePath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if !strings.HasSuffix(filePath, ".json") {
			// skip non-json file
			return nil
		}

		if strings.HasSuffix(filePath, ".meta.json") {
			// skip meta file
			return nil
		}

		secret, err := ReadLocalSecret(filePath)
		if err != nil {
			return fmt.Errorf("failed to read secret file: %s, %w", filePath, err)
		}

		vaultKey := toVaultKey(s.LocalPath, filePath, s.VaultPath)

		err = setKV(ctx, client, &VaultPair{
			MountPath: s.MountPath,
			Key:       vaultKey,
			Data:      secret.Data,
			Metadata:  secret.Metadata,
		}, s.CasTry)
		if err != nil {
			return fmt.Errorf("failed to set kv: %s, %w", vaultKey, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk local file: %w", err)
	}

	// delete kv
	err = WalkKV(ctx, client, s.VaultPath, s.MountPath, func(key string) error {
		localPath := ToLocalPath(s.LocalPath, s.VaultPath, key)
		exists, err := FileExists(localPath)
		if err != nil {
			return fmt.Errorf("failed to check local file: %s, %w", localPath, err)
		}
		metadataPath := toMetadataPath(localPath)
		metadataExists, err := FileExists(metadataPath)
		if err != nil {
			return fmt.Errorf("failed to check metadata file: %s, %w", metadataPath, err)
		}
		if exists {
			// local file exists, skip delete data
			if !metadataExists {
				// metadata not exists, clear remote metadata
				metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, key, vault.WithMountPath(s.MountPath))
				if err != nil {
					return fmt.Errorf("failed to read metadata: %s, %w", key, err)
				}
				if MetadataEqual(&metadataResponse.Data, nil) {
					// log.Printf("[%s] metadata unchanged", key)
					return nil
				}
				_, err = client.Secrets.KvV2WriteMetadata(ctx, key, schema.KvV2WriteMetadataRequest{
					CasRequired:        metadataResponse.Data.CasRequired,
					DeleteVersionAfter: metadataResponse.Data.DeleteVersionAfter,
					MaxVersions:        int32(metadataResponse.Data.MaxVersions),
					CustomMetadata: map[string]interface{}{
						"(empty)": "(empty)",
					},
				}, vault.WithMountPath(s.MountPath))
				if err != nil {
					return fmt.Errorf("failed to clear metadata: %s, %w", key, err)
				}
				log.Printf("[%s] delete metadata success", key)
				return nil
			} else {
				// metadata exists, skip delete data
				// log.Printf("[%s] skip delete data", key)
				return nil
			}
		} else {
			// local file not exists, delete remote file
			_, err = client.Secrets.KvV2Delete(ctx, key, vault.WithMountPath(s.MountPath))
			if err != nil {
				return fmt.Errorf("failed to delete kv: %s, %w", key, err)
			}
			_, err = client.Secrets.KvV2DeleteMetadataAndAllVersions(ctx, key, vault.WithMountPath(s.MountPath))
			if err != nil {
				return fmt.Errorf("failed to delete metadata: %s, %w", key, err)
			}
			log.Printf("[%s] delete data and metadata success", key)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk vault file: %w", err)
	}

	return nil
}

func WalkKV(ctx context.Context, client *vault.Client, vaultPath string, mountPath string, walkFn func(key string) error) error {
	response, err := client.Secrets.KvV2List(ctx, vaultPath, vault.WithMountPath(mountPath))
	if err != nil {
		return fmt.Errorf("failed to list kv: %s, %w", vaultPath, err)
	}

	for _, key := range response.Data.Keys {
		if strings.HasSuffix(key, "/") {
			// is a directory
			err := WalkKV(ctx, client, path.Join(vaultPath, key), mountPath, walkFn)
			if err != nil {
				return fmt.Errorf("failed to walk kv: %s, %w", path.Join(vaultPath, key), err)
			}
			continue
		}
		key = path.Join(vaultPath, key)
		if err := walkFn(key); err != nil {
			return fmt.Errorf("failed to walk kv: %s, %w", path.Join(vaultPath, key), err)
		}
	}

	return nil
}

func toVaultKey(prefix, localPath, vaultPath string) string {
	relativePath := strings.TrimPrefix(localPath, prefix)
	targetPath := path.Join(vaultPath, relativePath)
	targetPath = strings.TrimPrefix(targetPath, "/")
	targetPath = strings.TrimSuffix(targetPath, ".json")
	return targetPath
}

func toMetadataPath(path string) string {
	path = strings.TrimSuffix(path, ".json")
	return path + ".meta.json"
}

func ToLocalPath(localPath, vaultPath, key string) string {
	relativePath := strings.TrimPrefix(key, vaultPath)
	return path.Join(localPath, relativePath) + ".json"
}

func ReadJson(file string, v interface{}) error {
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("failed to open file: %s, %w", file, err)
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(v)
	if err != nil {
		return fmt.Errorf("failed to decode json: %s, %w", file, err)
	}
	return nil
}

func MapEqual(a, b map[string]interface{}) bool {
	if IsEmptyMap(a) && IsEmptyMap(b) {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func IsEmptyMap(a map[string]interface{}) bool {
	if len(a) == 0 {
		return true
	}
	if _, ok := a["(empty)"]; ok && len(a) == 1 {
		return true
	}
	return false
}

type Secret struct {
	Data     map[string]interface{} `json:"data,omitempty"`
	Metadata *schema.KvV2WriteMetadataRequest
}

func ReadData(file string) (map[string]interface{}, error) {
	var data = make(map[string]interface{})
	err := ReadJson(file, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func ReadMetadata(file string) (*schema.KvV2WriteMetadataRequest, error) {
	var metadata schema.KvV2WriteMetadataRequest
	err := ReadJson(file, &metadata)
	if err != nil {
		return nil, err
	}
	return &metadata, nil
}

func ReadLocalSecret(file string) (*Secret, error) {
	data, err := ReadData(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read local secret: %s, %w", file, err)
	}
	metadataPath := toMetadataPath(file)
	exists, err := FileExists(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check metadata file: %s, %w", metadataPath, err)
	}
	if !exists {
		return &Secret{
			Data: data,
		}, nil
	}
	metadata, err := ReadMetadata(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %s, %w", metadataPath, err)
	}
	return &Secret{
		Data:     data,
		Metadata: metadata,
	}, nil
}

type VaultPair struct {
	MountPath string
	Key       string
	Data      map[string]interface{} `json:"data,omitempty"`
	Metadata  *schema.KvV2WriteMetadataRequest
}

func setKV(ctx context.Context, client *vault.Client, pair *VaultPair, casTry int) error {
	for i := 0; i < casTry; i++ {
		err := trySetKV(ctx, client, pair)
		if err != nil {
			log.Printf("[%s] set kv failed, try %d: %+v", pair.Key, i+1, err)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to set kv after %d tries", casTry)
}

// POST https://vault-test.answeraiops.com/v1/kv/data/chatbot/admin/test
//
//	{
//		"data": {
//		  "test": "test"
//		},
//		"options": {
//		  "cas": 0
//		}
//	  }
//
// Response:
//
//	{
//	    "request_id": "ea63c36f-a83b-8b4c-fc9b-0a4173604cd7",
//	    "lease_id": "",
//	    "renewable": false,
//	    "lease_duration": 0,
//	    "data": {
//	        "created_time": "2024-12-24T11:36:20.976596673Z",
//	        "custom_metadata": null,
//	        "deletion_time": "",
//	        "destroyed": false,
//	        "version": 1
//	    },
//	    "wrap_info": null,
//	    "warnings": null,
//	    "auth": null,
//	    "mount_type": "kv"
//	}

// POST https://vault-test.answeraiops.com/v1/kv/metadata/chatbot/admin/test
//
//	{
//		"max_versions": 0,
//		"cas_required": true,
//		"delete_version_after": "0s",
//		"custom_metadata": {
//		  "test": "test"
//		}
//	  }

// DELETE https://vault-test.answeraiops.com/v1/kv/metadata/chatbot/admin/test
func trySetKV(ctx context.Context, client *vault.Client, pair *VaultPair) error {
	err := trySetData(ctx, client, pair)
	if err != nil {
		return fmt.Errorf("failed to set data: %w", err)
	}
	err = trySetMetadata(ctx, client, pair)
	if err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}
	return nil
}

func trySetData(ctx context.Context, client *vault.Client, pair *VaultPair) error {
	response, err := client.Secrets.KvV2Read(ctx, pair.Key, vault.WithMountPath(pair.MountPath))
	if err != nil {
		var responseError *vault.ResponseError
		if errors.As(err, &responseError) {
			if responseError.StatusCode == 404 {
				// not found
			} else {
				return fmt.Errorf("failed to read kv: %+v", responseError)
			}
		} else {
			return fmt.Errorf("failed to read kv: %w", err)
		}
	}

	if response == nil { // not found
		writeResponse, err := client.Secrets.KvV2Write(ctx, pair.Key, schema.KvV2WriteRequest{
			Data: pair.Data,
			Options: map[string]interface{}{
				"cas": 0,
			},
		}, vault.WithMountPath(pair.MountPath))
		if err != nil {
			return fmt.Errorf("failed to create data: %w", err)
		}

		log.Printf("[%s] create success (%d)", pair.Key, writeResponse.Data.Version)
		return nil
	}

	if MapEqual(response.Data.Data, pair.Data) {
		log.Printf("[%s] data unchanged", pair.Key)
		return nil
	}

	version, err := getVersion(response.Data.Metadata)
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	writeResponse, err := client.Secrets.KvV2Write(ctx, pair.Key, schema.KvV2WriteRequest{
		Data: pair.Data,
		Options: map[string]interface{}{
			"cas": version,
		},
	}, vault.WithMountPath(pair.MountPath))
	if err != nil {
		return fmt.Errorf("failed to update data: %w", err)
	}

	log.Printf("[%s] update success (%d)", pair.Key, writeResponse.Data.Version)
	return nil
}

func trySetMetadata(ctx context.Context, client *vault.Client, pair *VaultPair) error {
	var notFound bool
	metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, pair.Key, vault.WithMountPath(pair.MountPath))
	if err != nil {
		var notFoundError *vault.ResponseError
		if errors.As(err, &notFoundError) {
			if notFoundError.StatusCode == 404 {
				// not found
				notFound = true
			} else {
				return fmt.Errorf("failed to read metadata: %w", err)
			}
		} else {
			return fmt.Errorf("failed to read metadata: %w", err)
		}
	}

	if notFound {
		if pair.Metadata == nil {
			// not found and not set
			log.Printf("[%s] metadata not found and not set", pair.Key)
			return nil
		}
		// not found and set
		_, err = client.Secrets.KvV2WriteMetadata(ctx, pair.Key, *pair.Metadata, vault.WithMountPath(pair.MountPath))
		if err != nil {
			return fmt.Errorf("failed to create metadata: %w", err)
		}
		log.Printf("[%s] create metadata success", pair.Key)
		return nil
	}

	if MetadataEqual(&metadataResponse.Data, pair.Metadata) {
		log.Printf("[%s] metadata unchanged", pair.Key)
		return nil
	}

	if pair.Metadata == nil {
		// remove custom_metadata
		_, err = client.Secrets.KvV2WriteMetadata(ctx, pair.Key, schema.KvV2WriteMetadataRequest{
			CasRequired:        metadataResponse.Data.CasRequired,
			DeleteVersionAfter: metadataResponse.Data.DeleteVersionAfter,
			MaxVersions:        int32(metadataResponse.Data.MaxVersions),
			CustomMetadata: map[string]interface{}{
				"(empty)": "(empty)",
			},
		}, vault.WithMountPath(pair.MountPath))
		if err != nil {
			return fmt.Errorf("failed to clear metadata: %w", err)
		}
		log.Printf("[%s] clear metadata success", pair.Key)

		logMetadata(ctx, client, pair)
		return nil
	}

	_, err = client.Secrets.KvV2WriteMetadata(ctx, pair.Key, *pair.Metadata, vault.WithMountPath(pair.MountPath))
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}
	log.Printf("[%s] update metadata success", pair.Key)
	logMetadata(ctx, client, pair)
	return nil
}

func logMetadata(ctx context.Context, client *vault.Client, pair *VaultPair) {
	// metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, pair.Key, vault.WithMountPath(pair.MountPath))
	// if err != nil {
	// 	log.Printf("[%s] read metadata failed: %+v", pair.Key, err)
	// 	return
	// }
	// log.Printf("[%s] read metadata success (%+v) expect %+v", pair.Key, metadataResponse.Data, pair.Metadata)

	// response, err := client.Secrets.KvV2Read(ctx, pair.Key, vault.WithMountPath(pair.MountPath))
	// if err != nil {
	// 	log.Printf("[%s] read data failed: %+v", pair.Key, err)
	// 	return
	// }
	// log.Printf("[%s] read data success (%+v)", pair.Key, response.Data.Metadata)
}

func MetadataEqual(a *schema.KvV2ReadMetadataResponse, b *schema.KvV2WriteMetadataRequest) bool {
	if b == nil {
		return IsEmptyMap(a.CustomMetadata)
	}
	if a == nil {
		return IsEmptyMap(b.CustomMetadata)
	}
	if a.CasRequired != b.CasRequired {
		return false
	}
	if a.DeleteVersionAfter != b.DeleteVersionAfter {
		return false
	}
	if a.MaxVersions != int64(b.MaxVersions) {
		return false
	}

	return MapEqual(a.CustomMetadata, b.CustomMetadata)
}

func getVersion(metadata map[string]interface{}) (int, error) {
	version, ok := metadata["version"]
	if !ok {
		return 0, fmt.Errorf("version not found")
	}
	switch v := version.(type) {
	case int:
		return v, nil
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i), nil
		}
		if f, err := v.Float64(); err == nil {
			return int(f), nil
		}
		return 0, fmt.Errorf("version is not a number")
	case string:
		return strconv.Atoi(v)
	}
	return 0, fmt.Errorf("version is not a number")
}

func FileExists(file string) (bool, error) {
	_, err := os.Stat(file)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil // file does not exist
	}
	return false, err // file may or may not exist
}
