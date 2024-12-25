package syncer_test

import (
	"context"
	"testing"
	"time"

	"github.com/WqyJh/consul-vault-conf/test"
	"github.com/WqyJh/vaultsync/syncer"
	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/stretchr/testify/require"
)

func TestSync(t *testing.T) {
	ctx := context.Background()
	vaultServer, err := test.SetupVaultServer(ctx, test.VaultConfig{
		Policies: []test.VaultPolicy{
			{
				Name: "unittest-write",
				Policy: schema.PoliciesWriteAclPolicyRequest{
					Policy: `path "kv/data/unittest/*" {
						policy = "write"
					}`,
				},
			},
		},
		AppRoles: []test.VaultAppRole{
			{
				Name: "unittest",
				TokenRules: schema.AppRoleWriteRoleRequest{
					TokenPolicies: []string{"unittest-write"},
				},
			},
		},
	})
	require.NoError(t, err)
	defer vaultServer.Stop()

	for i := 0; i < 3; i++ {
		sync := syncer.NewSyncer(syncer.SyncerConfig{
			VaultAddr:  vaultServer.VaultAddr,
			VaultToken: vaultServer.RootToken,
			MountPath:  "kv",
			VaultPath:  "unittest",
			LocalPath:  "../testdata/dir1",
			CasTry:     3,
		})
		err = sync.Sync(ctx)
		require.NoError(t, err)
		t.Logf("sync dir1 success")

		client, err := vault.New(
			vault.WithAddress(vaultServer.VaultAddr),
			vault.WithRequestTimeout(30*time.Second),
		)
		require.NoError(t, err)

		err = client.SetToken(vaultServer.RootToken)
		require.NoError(t, err)

		count := 0
		err = syncer.WalkKV(ctx, client, "unittest", "kv", func(key string) error {
			count++

			filePath := syncer.ToLocalPath("../testdata/dir1", "unittest", key)
			secret, err := syncer.ReadLocalSecret(filePath)
			require.NoError(t, err)

			response, err := client.Secrets.KvV2Read(ctx, key, vault.WithMountPath("kv"))
			require.NoError(t, err)
			metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, key, vault.WithMountPath("kv"))
			require.NoError(t, err)
			require.True(t, syncer.MapEqual(secret.Data, response.Data.Data))
			require.True(t, syncer.MetadataEqual(&metadataResponse.Data, secret.Metadata))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, count)

		sync = syncer.NewSyncer(syncer.SyncerConfig{
			VaultAddr:  vaultServer.VaultAddr,
			VaultToken: vaultServer.RootToken,
			MountPath:  "kv",
			VaultPath:  "unittest",
			LocalPath:  "../testdata/dir2",
			CasTry:     3,
		})
		err = sync.Sync(ctx)
		require.NoError(t, err)
		t.Logf("sync dir2 success")
		count = 0
		err = syncer.WalkKV(ctx, client, "unittest", "kv", func(key string) error {
			count++

			filePath := syncer.ToLocalPath("../testdata/dir2", "unittest", key)
			secret, err := syncer.ReadLocalSecret(filePath)
			require.NoError(t, err)

			response, err := client.Secrets.KvV2Read(ctx, key, vault.WithMountPath("kv"))
			require.NoError(t, err)
			require.Equal(t, secret.Data, response.Data.Data)
			metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, key, vault.WithMountPath("kv"))
			require.NoError(t, err)
			require.True(t, syncer.MetadataEqual(&metadataResponse.Data, secret.Metadata))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)

		sync = syncer.NewSyncer(syncer.SyncerConfig{
			VaultAddr:  vaultServer.VaultAddr,
			VaultToken: vaultServer.RootToken,
			MountPath:  "kv",
			VaultPath:  "unittest",
			LocalPath:  "../testdata/dir3",
			CasTry:     3,
		})
		err = sync.Sync(ctx)
		require.NoError(t, err)
		t.Logf("sync dir3 success")
		count = 0
		err = syncer.WalkKV(ctx, client, "unittest", "kv", func(key string) error {
			count++

			filePath := syncer.ToLocalPath("../testdata/dir3", "unittest", key)
			secret, err := syncer.ReadLocalSecret(filePath)
			require.NoError(t, err)

			response, err := client.Secrets.KvV2Read(ctx, key, vault.WithMountPath("kv"))
			require.NoError(t, err)
			require.Equal(t, secret.Data, response.Data.Data)
			metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, key, vault.WithMountPath("kv"))
			require.NoError(t, err)
			require.True(t, syncer.MetadataEqual(&metadataResponse.Data, secret.Metadata))
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, count)
	}
}

func TestVault(t *testing.T) {
	ctx := context.Background()
	vaultServer, err := test.SetupVaultServer(ctx, test.VaultConfig{
		Policies: []test.VaultPolicy{
			{
				Name: "unittest-write",
				Policy: schema.PoliciesWriteAclPolicyRequest{
					Policy: `path "kv/data/unittest/*" {
						policy = "write"
					}`,
				},
			},
		},
		AppRoles: []test.VaultAppRole{
			{
				Name: "unittest",
				TokenRules: schema.AppRoleWriteRoleRequest{
					TokenPolicies: []string{"unittest-write"},
				},
			},
		},
	})
	require.NoError(t, err)
	defer vaultServer.Stop()

	client, err := vault.New(
		vault.WithAddress(vaultServer.VaultAddr),
		vault.WithRequestTimeout(30*time.Second),
	)
	require.NoError(t, err)

	err = client.SetToken(vaultServer.RootToken)
	require.NoError(t, err)

	response, err := client.Secrets.KvV2Write(ctx, "unittest/test1", schema.KvV2WriteRequest{
		Data: map[string]interface{}{
			"test": "test1",
		},
		Options: map[string]interface{}{
			"cas": 0,
		},
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	require.Equal(t, int64(1), response.Data.Version)

	writeMetadataResponse, err := client.Secrets.KvV2WriteMetadata(ctx, "unittest/test1", schema.KvV2WriteMetadataRequest{
		CasRequired:        true,
		DeleteVersionAfter: "0s",
		MaxVersions:        0,
		CustomMetadata: map[string]interface{}{
			"test": "test1",
		},
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("writeMetadataResponse: %+v", writeMetadataResponse)

	readResponse, err := client.Secrets.KvV2Read(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("readResponse: %+v", readResponse)
	require.Equal(t, "test1", readResponse.Data.Data["test"])

	metadataResponse, err := client.Secrets.KvV2ReadMetadata(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("metadataResponse: %+v", metadataResponse)
	require.Equal(t, map[string]interface{}{
		"test": "test1",
	}, metadataResponse.Data.CustomMetadata)

	_, err = client.Secrets.KvV2DeleteMetadataAndAllVersions(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)

	_, err = client.Secrets.KvV2ReadMetadata(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.Error(t, err)
	var notFoundError *vault.ResponseError
	require.ErrorAs(t, err, &notFoundError)
	require.Equal(t, 404, notFoundError.StatusCode)

	_, err = client.Secrets.KvV2Delete(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)

	_, err = client.Secrets.KvV2Read(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.Error(t, err)
	require.ErrorAs(t, err, &notFoundError)
	require.Equal(t, 404, notFoundError.StatusCode)

	response, err = client.Secrets.KvV2Write(ctx, "unittest/test1", schema.KvV2WriteRequest{
		Data: map[string]interface{}{
			"test": "test2",
		},
		Options: map[string]interface{}{
			"cas": 0,
		},
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	require.Equal(t, int64(1), response.Data.Version)

	writeMetadataResponse, err = client.Secrets.KvV2WriteMetadata(ctx, "unittest/test1", schema.KvV2WriteMetadataRequest{
		CasRequired:        true,
		DeleteVersionAfter: "0s",
		MaxVersions:        0,
		CustomMetadata: map[string]interface{}{
			"test": "test2",
		},
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("writeMetadataResponse: %+v", writeMetadataResponse)

	readResponse, err = client.Secrets.KvV2Read(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("readResponse: %+v", readResponse)
	require.Equal(t, "test2", readResponse.Data.Data["test"])

	metadataResponse, err = client.Secrets.KvV2ReadMetadata(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("metadataResponse: %+v", metadataResponse)
	require.Equal(t, map[string]interface{}{
		"test": "test2",
	}, metadataResponse.Data.CustomMetadata)

	// second write metadata

	response, err = client.Secrets.KvV2Write(ctx, "unittest/test1", schema.KvV2WriteRequest{
		Data: map[string]interface{}{
			"test": "test3",
		},
		Options: map[string]interface{}{
			"cas": response.Data.Version,
		},
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	require.Equal(t, int64(2), response.Data.Version)

	kvListResponse, err := client.Secrets.KvV2List(ctx, "unittest", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("kvListResponse: %+v", kvListResponse)

	writeMetadataResponse, err = client.Secrets.KvV2WriteMetadata(ctx, "unittest/test1", schema.KvV2WriteMetadataRequest{
		CasRequired:        true,
		DeleteVersionAfter: "0s",
		MaxVersions:        0,
		CustomMetadata:     map[string]interface{}{}, // same as nil
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("writeMetadataResponse: %+v", writeMetadataResponse)

	readResponse, err = client.Secrets.KvV2Read(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("readResponse: %+v", readResponse)
	require.Equal(t, "test3", readResponse.Data.Data["test"])

	metadataResponse, err = client.Secrets.KvV2ReadMetadata(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("metadataResponse: %+v", metadataResponse)
	// Attention:
	// This is vault bug, custom_metadata should be empty, but it is not.
	// Provide an empty map or null to KvV2WriteMetadata should be able
	// to clear custom_metadata, however, it is not working.
	// It seems that after you write metadata for twice, then you can't
	// clear it by writing empty map or null to KvV2WriteMetadata,
	// but you can always update it by non-empty map.
	// require.Equal(t, map[string]interface{}(nil), metadataResponse.Data.CustomMetadata)
	require.Equal(t, map[string]interface{}{
		"test": "test2",
	}, metadataResponse.Data.CustomMetadata)
	// We have to use this workaround to clear custom_metadata
	_, err = client.Secrets.KvV2WriteMetadata(ctx, "unittest/test1", schema.KvV2WriteMetadataRequest{
		CasRequired:        metadataResponse.Data.CasRequired,
		DeleteVersionAfter: metadataResponse.Data.DeleteVersionAfter,
		MaxVersions:        int32(metadataResponse.Data.MaxVersions),
		CustomMetadata: map[string]interface{}{
			"(empty)": "(empty)",
		},
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)

	metadataResponse, err = client.Secrets.KvV2ReadMetadata(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("metadataResponse: %+v", metadataResponse)
	require.Equal(t, map[string]interface{}{
		"(empty)": "(empty)",
	}, metadataResponse.Data.CustomMetadata)

	// try clear again
	writeMetadataResponse, err = client.Secrets.KvV2WriteMetadata(ctx, "unittest/test1", schema.KvV2WriteMetadataRequest{
		CasRequired:        true,
		DeleteVersionAfter: "0s",
		MaxVersions:        0,
		CustomMetadata:     map[string]interface{}{}, // same as nil
	}, vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("writeMetadataResponse: %+v", writeMetadataResponse)

	// not cleared again
	metadataResponse, err = client.Secrets.KvV2ReadMetadata(ctx, "unittest/test1", vault.WithMountPath("kv"))
	require.NoError(t, err)
	t.Logf("metadataResponse: %+v", metadataResponse)
	require.Equal(t, map[string]interface{}{
		"(empty)": "(empty)",
	}, metadataResponse.Data.CustomMetadata)
}
