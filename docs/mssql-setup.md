# SQL Server Setup Options

This API supports two production patterns for existing Microsoft SQL Server deployments.

## Option A (works regardless of AD/Entra state)

Use SQL auth with strong network + TLS controls.

### 1) Keep SQL private-only, only API host -> DB host:1433

- Self-managed SQL Server (Windows): restrict inbound 1433 to only the API host (or VNet/subnet) in firewall/security group.
- Azure SQL: disable public access and use private networking.

Microsoft docs:

- Windows Firewall for SQL Server access: <https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-a-windows-firewall-for-database-engine-access?view=sql-server-ver17>
- Azure SQL connectivity settings (public/private access): <https://learn.microsoft.com/en-us/azure/azure-sql/database/connectivity-settings?view=azuresql>
- Azure SQL Private Endpoint: <https://learn.microsoft.com/en-us/azure/azure-sql/database/private-endpoint-overview?view=azuresql>

### 2) Enforce TLS with certificate validation

- Install a valid server certificate and configure SQL Server encryption posture.
- In this API set:
  - `DB_ENCRYPT=true`
  - `DB_TRUST_SERVER_CERTIFICATE=false`

Microsoft docs:

- Configure SQL Server encryption/TLS: <https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-sql-server-encryption?view=sql-server-ver16>
- `Encrypt` and `TrustServerCertificate` semantics: <https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax?view=sql-server-ver16>

### 3) Use a dedicated app login (not `sa`) with least privilege

- Create a server login dedicated to this API.
- Create a database user mapped to that login.
- Grant only required rights in the target DB.

Microsoft docs:

- Create login: <https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/create-a-login?view=sql-server-ver16>
- Create database user: <https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/create-a-database-user?view=sql-server-ver16>
- Permissions model (least privilege): <https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/getting-started-with-database-engine-permissions?view=sql-server-ver16>

### 4) Keep credentials out of source control

- Inject `DB_PASSWORD` from your secret manager/CI runtime environment.
- Never commit credentials in repo, compose files, or scripts.

Microsoft docs:

- Azure Key Vault secrets quickstart: <https://learn.microsoft.com/en-us/azure/key-vault/secrets/quick-create-portal>

## Option B (when Entra capability is confirmed)

Use passwordless auth with Microsoft Entra identity.

### 1) Confirm your SQL target supports Entra auth

- Azure SQL Database / Managed Instance: supported.
- SQL Server 2022: supported in specific deployment scenarios.

Microsoft docs:

- Microsoft Entra auth for SQL Server 2022: <https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/microsoft-entra-authentication-sql-server-overview?view=sql-server-ver17>
- Microsoft Entra with Azure SQL (service principals/managed identity): <https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-azure-ad-service-principal-tutorial?view=azuresql>

### 2) Keep the same private network and TLS posture

- Keep Option A network controls (private-only DB reachability).
- Keep TLS encryption and certificate validation (`TrustServerCertificate=false`).

### 3) App-side note

This repo currently uses SQL authentication connection strings for SQL Server mode.
If you choose Entra auth, add an Entra-capable SQL connection flow (token-based auth) before switching production traffic.
