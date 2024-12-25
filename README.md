# vaultsync

Sync local path to vault

## Install

```bash
go install github.com/WqyJh/vaultsync@latest
```

## Usage

```bash
vaultsync -local-path path/to/local \
-vault-path path/to/vault \
-vault-addr http://127.0.0.1:8500 \
-vault-token your_token \
-mount-path kv
```
