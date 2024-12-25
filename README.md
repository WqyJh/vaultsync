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
vaultsync -vault-addr http://127.0.0.1:8200 \
-vault-token your_token \
-mount-path kv \
-local-path path/to/local \
-vault-path path/to/vault
```

Use app role to login vault

```bash
vaultsync -vault-addr http://127.0.0.1:8200 \
-role-id role_id \
-secret-id secret_id \
-mount-path kv \
-local-path path/to/local \
-vault-path path/to/vault
```

## Fetch

Fetch vault secrets to local path.

```bash
vaultfetch -vault-addr http://127.0.0.1:8200 \
-vault-token your_token \
-mount-path kv \
-vault-path path/to/vault \
-local-path path/to/local
```

Use app role to login vault

```bash
vaultfetch -vault-addr http://127.0.0.1:8200 \
-role-id role_id \
-secret-id secret_id \
-mount-path kv \
-vault-path path/to/vault \
-local-path path/to/local
```
