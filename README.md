# vaultsync

Sync local path to vault

## Install

Install vaultsync

```bash
go install github.com/WqyJh/vaultsync/cmd/vaultsync@latest
```

Install vaultfetch

```bash
go install github.com/WqyJh/vaultsync/cmd/vaultfetch@latest
```

## Usage

```bash
vaultsync -vault-addr http://127.0.0.1:8500 \
-vault-token your_token \
-mount-path kv \
-local-path path/to/local \
-vault-path path/to/vault
```

## Fetch

Fetch vault secrets to local path.

```bash
vaultfetch -vault-addr http://127.0.0.1:8500 \
-vault-token your_token \
-mount-path kv \
-vault-path path/to/vault \
-local-path path/to/local
```
