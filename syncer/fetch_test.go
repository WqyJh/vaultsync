package syncer_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/WqyJh/consul-vault-conf/test"
	"github.com/WqyJh/vaultsync/syncer"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/stretchr/testify/require"
)

func TestFetch(t *testing.T) {

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

	fetcher := syncer.NewFetcher(syncer.SyncerConfig{
		VaultAddr:  vaultServer.VaultAddr,
		VaultToken: vaultServer.RootToken,
		MountPath:  "kv",
		VaultPath:  "unittest",
		LocalPath:  "/tmp/testdata/dir1",
		CasTry:     3,
	})
	err = fetcher.Fetch(ctx)
	require.NoError(t, err)

	directoryEqual(t, "../testdata/dir1", "/tmp/testdata/dir1")
}

func fileEqual(t *testing.T, file1, file2 string, isMetadata bool) {
	if isMetadata {
		metadata1, err := syncer.ReadMetadata(file1)
		require.NoError(t, err)
		metadata2, err := syncer.ReadMetadata(file2)
		require.NoError(t, err)
		require.Equal(t, metadata1, metadata2)
		t.Logf("metadata equal: %s, %s", file1, file2)
	} else {
		data1, err := syncer.ReadData(file1)
		require.NoError(t, err)
		data2, err := syncer.ReadData(file2)
		require.NoError(t, err)
		require.Equal(t, data1, data2)
		t.Logf("data equal: %s, %s", file1, file2)
	}
}

func directoryEqual(t *testing.T, dir1, dir2 string) {
	entries1, err := os.ReadDir(dir1)
	require.NoError(t, err)
	entries2, err := os.ReadDir(dir2)
	require.NoError(t, err)

	var dirs1, dirs2 []os.DirEntry
	var files1, files2 []os.DirEntry

	for _, entry := range entries1 {
		if entry.IsDir() {
			subEntries, err := os.ReadDir(filepath.Join(dir1, entry.Name()))
			require.NoError(t, err)
			if len(subEntries) == 0 {
				continue
			}

			dirs1 = append(dirs1, entry)
		} else if strings.HasSuffix(entry.Name(), ".json") {
			files1 = append(files1, entry)
		}
	}

	for _, entry := range entries2 {
		if entry.IsDir() {
			subEntries, err := os.ReadDir(filepath.Join(dir2, entry.Name()))
			require.NoError(t, err)
			if len(subEntries) == 0 {
				continue
			}
			dirs2 = append(dirs2, entry)
		} else if strings.HasSuffix(entry.Name(), ".json") {
			files2 = append(files2, entry)
		}
	}

	require.Equal(t, len(dirs1), len(dirs2))
	require.Equal(t, len(files1), len(files2))

	sortFunc := func(a, b os.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	}

	slices.SortFunc(files1, sortFunc)
	slices.SortFunc(files2, sortFunc)
	slices.SortFunc(dirs1, sortFunc)
	slices.SortFunc(dirs2, sortFunc)

	for i := range files1 {
		f1 := files1[i]
		f2 := files2[i]
		if strings.HasSuffix(f1.Name(), ".meta.json") {
			fileEqual(t, filepath.Join(dir1, f1.Name()), filepath.Join(dir2, f2.Name()), true)
		} else if strings.HasSuffix(f1.Name(), ".json") {
			fileEqual(t, filepath.Join(dir1, f1.Name()), filepath.Join(dir2, f2.Name()), false)
		}
	}

	for i := range dirs1 {
		d1 := dirs1[i]
		d2 := dirs2[i]
		directoryEqual(t, filepath.Join(dir1, d1.Name()), filepath.Join(dir2, d2.Name()))
	}
}
