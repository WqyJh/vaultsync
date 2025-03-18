package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
)

type Fetcher struct {
	SyncerConfig
}

func NewFetcher(config SyncerConfig) *Fetcher {
	config.VaultPath = strings.TrimPrefix(path.Clean(config.VaultPath), "/")
	config.LocalPath = path.Clean(config.LocalPath)
	return &Fetcher{SyncerConfig: config}
}

func (f *Fetcher) Fetch(ctx context.Context) error {
	client, err := vault.New(
		vault.WithAddress(f.VaultAddr),
		vault.WithRequestTimeout(30*time.Second),
		vault.WithEnvironment(),
	)
	if err != nil {
		return fmt.Errorf("failed to create vault client: %w", err)
	}

	if f.VaultToken == "" {
		response, err := client.Auth.AppRoleLogin(ctx, schema.AppRoleLoginRequest{
			RoleId:   f.VaultRoleId,
			SecretId: f.VaultSecretId,
		})
		if err != nil {
			return fmt.Errorf("failed to login with app role: %w", err)
		}
		f.VaultToken = response.Auth.ClientToken
	}

	err = client.SetToken(f.VaultToken)
	if err != nil {
		return fmt.Errorf("failed to set vault token: %w", err)
	}

	err = WalkKV(ctx, client, f.VaultPath, f.MountPath, func(key string) error {
		localPath := ToLocalPath(f.LocalPath, f.VaultPath, key)
		err := os.MkdirAll(path.Dir(localPath), 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %s, %w", path.Dir(localPath), err)
		}
		file, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create file: %s, %w", localPath, err)
		}
		defer file.Close()

		response, err := client.Secrets.KvV2Read(ctx, key, vault.WithMountPath(f.MountPath))
		if err != nil {
			return fmt.Errorf("failed to read secret: %s, %w", key, err)
		}

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "    ")
		err = encoder.Encode(response.Data.Data)
		if err != nil {
			return fmt.Errorf("failed to save data: %s, %w", key, err)
		}

		log.Printf("[%s] fetch success", key)

		metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, key, vault.WithMountPath(f.MountPath))
		if err != nil {
			return fmt.Errorf("failed to read metadata: %s, %w", key, err)
		}

		metadataPath := toMetadataPath(localPath)
		if IsEmptyMap(metadataResponse.Data.CustomMetadata) {
			return nil
		}

		metadataFile, err := os.Create(metadataPath)
		if err != nil {
			return fmt.Errorf("failed to create metadata file: %s, %w", metadataPath, err)
		}
		defer metadataFile.Close()

		metadataRequest := schema.KvV2WriteMetadataRequest{
			CasRequired:        metadataResponse.Data.CasRequired,
			DeleteVersionAfter: metadataResponse.Data.DeleteVersionAfter,
			MaxVersions:        int32(metadataResponse.Data.MaxVersions),
			CustomMetadata:     metadataResponse.Data.CustomMetadata,
		}

		encoder = json.NewEncoder(metadataFile)
		encoder.SetIndent("", "    ")
		err = encoder.Encode(metadataRequest)
		if err != nil {
			return fmt.Errorf("failed to save metadata: %s, %w", key, err)
		}

		log.Printf("[%s] metadata save success", key)

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk kv: %w", err)
	}
	return nil
}
