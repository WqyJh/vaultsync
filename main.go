package main

import (
	"context"
	"flag"
	"log"

	"github.com/WqyJh/vaultsync/syncer"
)

var (
	localPath  = flag.String("local-path", "", "path of the local files")
	vaultPath  = flag.String("vault-path", "", "path of the vault files")
	vaultAddr  = flag.String("vault-addr", "", "vault address")
	vaultToken = flag.String("vault-token", "", "vault token")
	mountPath  = flag.String("mount-path", "", "mount path")
	casTry     = flag.Int("cas-try", 3, "number of times to try cas")
)

func main() {
	flag.Parse()

	syncer := syncer.NewSyncer(syncer.SyncerConfig{
		VaultAddr:  *vaultAddr,
		VaultToken: *vaultToken,
		MountPath:  *mountPath,
		VaultPath:  *vaultPath,
		LocalPath:  *localPath,
		CasTry:     *casTry,
	})
	err := syncer.Sync(context.Background())
	if err != nil {
		log.Fatalf("failed to sync: %+v", err)
	}
}
