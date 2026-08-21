# RoboSystems Security

Security controls implemented at the infrastructure, application, and data levels.

Live compliance posture and audit artifacts are published in the [RoboSystems Trust Center](https://trust.robosystems.ai).

## Authentication and Access Control

### JWT Authentication

**Implementation:** `robosystems/middleware/auth/jwt.py`

- 30-minute access token expiration (`JWT_EXPIRY_HOURS` in `robosystems/config/constants.py`; a code constant, not an environment variable)
- JTI-based token revocation tracked in Valkey, with TTL equal to the token's remaining lifetime
- Token refresh at `/v1/auth/refresh`: the old token is revoked with a 5-second grace period for in-flight requests; tokens expired by up to 5 minutes are still accepted for refresh, subject to device-fingerprint and revocation checks
- Issuer and audience claim validation
- Token purpose enforcement: single-use SSO handoff tokens and MFA challenge tokens are refused as session bearers
- Every token carries the user's `session_version`; password change, password reset, and account deactivation bump it, invalidating all outstanding tokens (deactivation also revokes the user's API keys)
- Fails closed on Valkey errors (treats token as revoked)

### Device Fingerprinting

**Implementation:** `robosystems/security/device_fingerprinting.py`

- Browser binding: a SHA256 hash of the `User-Agent`, `Accept-Language`, `Accept-Encoding`, `Sec-CH-UA`, and `Sec-CH-UA-Platform` headers is embedded in the JWT at issuance
- Intentionally excludes IP address (VPN/mobile/load balancer changes)
- Any change to the bound headers invalidates the token, including on grace-period refresh

### API Key Management

**Implementation:** `robosystems/models/core/user/user_api_key.py`

- Cryptographically secure generation (`secrets.token_hex`): `rfs` prefix + 64 hex characters; graph-scoped keys use the `rfsc` prefix
- Bcrypt hashing with cost factor 12 for storage; the plaintext is returned once at creation and never stored
- Optional graph scoping (`graph_id`): a scoped key is valid only for that graph and its subgraphs
- Optional per-key expiration (`expires_at`)
- Usage tracking via `last_used_at` timestamp
- Validation results are cached in Valkey (encrypted and signed; see Application-Level Encryption); deactivating or deleting a key invalidates its cache entry

### Authentication Protection

**Implementation:** `robosystems/security/auth_protection.py`

- Progressive delays on failed attempts (1s, 2s, 5s, 10s, 30s, 1m, 5m, 15m cap), enforced as `429` responses with `Retry-After`
- IP-based threat assessment with four levels:
  - LOW: <5 failures
  - MEDIUM: 5+ failures (15-minute block)
  - HIGH: 10+ failures (1-hour block)
  - CRITICAL: 20+ failures (24-hour block)
- Blocks automatically expire; the per-IP failure count decrements by one on each successful login, and the record expires 25 hours after the last attempt
- Stored in Valkey under SHA256-hashed IP keys; graceful degradation on cache failure
- Applied to the login, registration, and MFA endpoints; passkey login failures count toward the same per-IP record

### Passkeys (WebAuthn) and MFA

**Implementation:** `robosystems/routers/auth/passkeys.py`, `robosystems/routers/auth/mfa.py`, `robosystems/operations/passkeys.py`, `robosystems/models/core/user/user_passkey.py`

- WebAuthn passkeys serve as a second factor after password login (`/v1/auth/mfa/*`) and as a passwordless first factor (`/v1/auth/passkeys/login/*`)
- Gated by `PASSKEYS_ENABLED`; `MFA_ENFORCEMENT_ENABLED` additionally requires org owners and admins to enroll a passkey before a password login yields a session
- Enrollment requires a fresh re-authentication proof on top of the session (API keys are refused), or a purpose-scoped `enroll` MFA token in the forced-enrollment lane
- MFA challenge tokens are short-lived (5 minutes), purpose-scoped (`login` vs `enroll`), and refused as session bearers
- Ten single-use recovery codes back the second factor (stored as SHA256 hashes; regenerable at `/v1/auth/mfa/recovery-codes/regenerate`)
- Relying Party ID and origin derive from the deployment's root domain (`PASSKEY_RP_ID` / `PASSKEY_ORIGIN` override); stored credentials hold only the public key, sign counter, transports, and backup flags

### Enterprise SSO (OIDC) and SCIM Provisioning

**Implementation:** `robosystems/routers/auth/oidc.py`, `robosystems/operations/oidc.py`, `robosystems/routers/scim/`, `robosystems/middleware/auth/scim.py`, `robosystems/models/core/user/scim_token.py`

- OIDC authorization-code login against one configured identity provider (`SSO_OIDC_ENABLED`, `SSO_OIDC_ISSUER`, `SSO_OIDC_CLIENT_ID`), with provider discovery, single-use 10-minute flow state carrying a nonce and PKCE verifier, and ID-token validation
- Link-only: SSO never creates accounts; first login binds to the SCIM-provisioned user through a configurable ID-token claim (`SSO_OIDC_BINDING_CLAIM`, default `sub`) and requires membership in the pinned enterprise org (`ENTERPRISE_ORG_ID`)
- SCIM 2.0 user provisioning at `/scim/v2` (`SCIM_ENABLED`, gated independently of OIDC): `/Users` CRUD plus `ServiceProviderConfig`, `ResourceTypes`, and `Schemas`
- SCIM bearer tokens are per-org, bcrypt-hashed, shown once at mint, expiring (365-day default), and rotated by overlap; they authenticate provisioning only — never accepted by the normal auth dependencies, while user JWTs and API keys are never accepted at the SCIM surface
- `/v1/auth/providers` publishes which auth methods a deployment offers (`PASSWORD_AUTH_ENABLED`, `SSO_OIDC_ENABLED`) so clients render the login page from runtime config

### CAPTCHA Verification

**Implementation:** `robosystems/security/captcha.py`

- Cloudflare Turnstile server-side token verification
- Configurable via `CAPTCHA_ENABLED` environment variable; when enabled in staging or production without a Turnstile secret configured, verification fails closed
- Applied to registration (`/v1/auth/register`); `/v1/auth/captcha/config` tells clients whether a token is required and which site key to use

### Password Security

**Implementation:** `robosystems/security/password.py`

- Bcrypt hashing with 14 rounds, run off the event loop
- Score-based strength validation (min 60/100 to pass)
- Pattern detection: sequential chars, repeated chars, keyboard patterns, common passwords, and substrings of the user's own email address
- Requirements: 12+ chars (128 max), uppercase, lowercase, digit, special character, 8+ unique characters
- Login timing is equalized: a miss (unknown email, inactive account) performs the same bcrypt work as a real verification, so response time does not reveal whether an address is registered

### Multi-Tenant Access Control

**Implementation:** `robosystems/models/core/graph/graph_user.py`, `robosystems/models/core/org/org_user.py`

- Graphs are owned by organizations; org roles are owner, admin, and member
- Graph roles: admin (full control), member (read/write, default), viewer (read-only); org owners and admins hold implicit admin on every graph their org owns
- One role resolver (`GraphUser.get_effective_role`) backs REST, GraphQL, MCP, and the extensions surface; a deprovisioned or deleted graph resolves to no access for everyone, including org owners and admins
- Graph data: a separate LadybugDB database per graph (and per subgraph); extensions OLTP data (RoboLedger/RoboInvestor): a dedicated PostgreSQL schema per graph, selected with `SET search_path`
- Subgraph permissions inherited from parent graph
- All graph endpoints scoped by `graph_id`

## Data Security

### Encryption at Rest

- PostgreSQL: AES-256 encryption via AWS RDS; 30-day automated backups; Multi-AZ optional (`DATABASE_MULTI_AZ_ENABLED_{PROD,STAGING}`, off by default)
- LadybugDB: EBS volume encryption
- S3: AES256 server-side encryption (SSE-S3) and public-access block on all buckets; Object Lock (governance mode, 400 days) on the audit and CloudTrail buckets, applied through the S3 API (see the template notes)
- OpenSearch: encryption at rest and node-to-node encryption
- Valkey: encryption at rest and in transit (default on), AUTH token from Secrets Manager
- Graph backups: SSE-AES256 on the stored object; served only over TLS through
  signed URLs (1-hour default; caller-selectable from 5 minutes to 24 hours); 90-day
  retention by default. Not additionally encrypted at the application layer —
  a backup download is a usable `.lbug` database by design.

### Encryption in Transit

- API load balancer: HTTPS-only listener, TLS 1.2/1.3 (`ELBSecurityPolicy-TLS13-1-2-FIPS-2023-04`)
- CloudFront: `TLSv1.2_2021` minimum protocol when a custom domain is configured; HTTPS redirect always
- SSL/TLS required for all database connections (`rds.force_ssl: 1`; RDS Proxy `RequireTLS`); OpenSearch enforces HTTPS with a TLS 1.2 minimum
- Certificate management via AWS Certificate Manager

### Application-Level Encryption

**Implementation:** `robosystems/models/core/connection/connection_credentials.py`, `robosystems/middleware/auth/cache.py`

- Fernet encryption for OAuth tokens and connection credentials in PostgreSQL
- Fernet encryption for the authentication cache, with daily key rotation
- Encryption keys stored in AWS Secrets Manager (not environment variables in prod)

### Secrets Management

**Implementation:** `robosystems/config/secrets_manager.py`

- AWS Secrets Manager integration with hierarchical secret organization
- Base secret (`robosystems/{env}`) plus extension secrets (`robosystems/{env}/{component}`)
- TTL-based caching (1-hour default) with explicit `refresh()` invalidation
- ~36 secrets mapped (JWT, database, S3, Stripe, Intuit, etc.); feature flags live in SSM Parameter Store, not Secrets Manager
- Graceful fallback to environment variables in development

### Secrets Rotation

**Implementation:** `.github/workflows/secrets-rotation.yml`

- Monthly schedule via GitHub Actions (staging: 1st of month, prod: 2nd of month)
- Lambda-based rotation functions (PostgreSQL, Valkey, and one shared API-key function for the Graph API and Admin keys)
- Rotated secrets: PostgreSQL password, Valkey auth token, Graph API key, Admin API key
- Pre-rotation cleanup for stuck AWSPENDING versions
- Post-rotation verification with timeout handling (5 min for Postgres/API keys, 30 min for Valkey)
- Automatic service refresh after successful rotation
- SNS email notifications on completion
- Controlled by `SECRETS_ROTATION_ENABLED_STAGING` and `SECRETS_ROTATION_ENABLED_PROD` GitHub variables (disabled by default)

## Application Security

### Input Validation

**Implementation:** `robosystems/security/input_validation.py` + Pydantic models

- HTML escape and dangerous character removal (`<>"\'\0\r\n`)
- Email, username, UUID, URL, and SQL identifier validation
- Recursive sanitization of nested dicts/lists
- Pydantic `EmailStr`, `min_length`/`max_length` constraints, and custom validators at API boundaries

### Query Security

**Implementation:** `robosystems/security/cypher_analyzer.py`

- AST-based Cypher query analysis (comment/string removal before keyword detection)
- Keyword detection across four sets — write (CREATE, MERGE, SET, DELETE, …), bulk, admin, and schema DDL — plus procedure calls outside a read-only allowlist
- Operation classification: READ, WRITE, or MIXED; bulk, admin, and schema-DDL statements are flagged by separate checks
- Suspicious pattern detection (USER creation, DATABASE drops, `dbms.*` calls)
- Query length limit (100KB max) for DoS prevention
- Fails closed: analysis failure defaults to `is_write_operation=True`

### Secure Error Handling

**Implementation:** `robosystems/security/error_handling.py`

- Generic error messages to clients (e.g., "Access denied" not DB errors)
- Sensitive pattern detection (20+ patterns: password, secret, token, traceback, etc.)
- Full internal logging with original exceptions and stack traces
- Error classification with appropriate HTTP status codes

### Rate Limiting

**Implementation:** `robosystems/middleware/rate_limits/rate_limiting.py`

- Distributed rate limiting using Valkey with sliding window algorithm
- Subscription-aware: dedicated-resource categories scale with instance vCPU, using Standard as the
  anchor (Large 2x, XLarge 4x). Shared categories — auth, status, billing, SSE — are identical on
  every tier
- Per-endpoint category limits (22 categories: auth, graph_read, graph_write, etc.)
- User identification: API key (SHA256 hash) > JWT (user_id) > IP (fallback)
- Response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Tier`, `X-RateLimit-Category`; a 429 additionally carries `X-RateLimit-Reset` and `Retry-After`
- Sensitive auth endpoints: login (5/5min) and register (3/hour), keyed by client IP; JWT refresh (20/min), keyed by the caller's identity while its token still parses and by client IP once it does not
- Auth endpoints fail closed: if the limiter backend is unavailable, login and registration are denied rather than left unprotected

### Admission Control

**Implementation:** `robosystems/middleware/graph/admission_control.py`

- Memory, CPU, and queue pressure thresholds (tunable via SSM)
- Load shedding with configurable start/stop pressure thresholds
- Applied to both main API and Graph API (LadybugDB)

### File Upload Security

**Implementation:** `robosystems/operations/graph/commands/create_file_upload.py`, `robosystems/operations/graph/commands/ingest_file.py` (routed through `robosystems/routers/graphs/content_ops.py`)

- Presigned S3 URLs for time-limited direct uploads (no API bottleneck)
- File format validation (parquet, csv, json)
- Per-file size limit (`MAX_FILE_SIZE_MB`) checked at upload; tier storage cap enforced at ingest
- Parquet row counts read from the file footer at ingest (the object is never fully decoded)

## Infrastructure Security

### Network Security

**Implementation:** `cloudformation/vpc.yaml`

- Private/public subnet segmentation across 2–6 availability zones (default 5)
- NAT Gateway for private subnet outbound access
- Security groups with least-privilege access
- VPC endpoints for S3 and DynamoDB (gateway, free); Secrets Manager, ECR API, and ECR Docker (interface)
- No direct internet access to application/data tiers

### Web Application Firewall (WAF)

**Implementation:** `cloudformation/waf.yaml`

- Rate limiting: 3,000 requests per 5-minute window per IP (configurable 100-20,000)
- SQL injection protection (query args, request body, URI path)
- Cross-site scripting (XSS) protection
- Payload size restrictions (8MB body, 8KB header)
- AWS Managed Core Rule Set (optional, enabled by default)
- Optional geographic blocking (US/Canada only)
- IP allowlist for bypassing rules
- Custom JSON error responses (429 rate limit, 403 attack block, 413 payload size)

### Application Load Balancer

- SSL termination with health checks
- Admin API paths answered with a fixed `403` at the listener; the admin surface is reachable only through the SSM bastion tunnel

### CI/CD Security (OIDC)

**Implementation:** `cloudformation/bootstrap-oidc.yaml`

- GitHub OIDC federation (no long-term AWS credentials)
- Federated role restricted to `main` branch, `release/*` branches, and `v*` tags
- Scoped to specific repositories (robosystems, robosystems-app, roboledger-app, roboinvestor-app, robosystems-holon-viewer)
- 1-hour maximum session duration
- Permission scoping: ECR limited to `robosystems*`, S3 limited to `robosystems-*`

### Infrastructure as Code

- CloudFormation templates for all infrastructure
- Automated template validation in CI/CD
- Version-controlled infrastructure changes

## Security Monitoring

### Audit Logging

**Implementation:** `robosystems/security/audit_logger.py`

- Structured JSON logging with 46 event types, including:
  - Authentication: success, failure, token expired/invalid
  - Authorization: denied, privilege escalation attempt
  - Security: injection attempt, rate limit exceeded, suspicious activity
  - Operations: data import, timeout, financial transaction
  - Identity lifecycle: OIDC login denied, SCIM provisioning, org and graph membership changes, passkey enrollment, MFA challenges
- Risk level classification: LOW, MEDIUM, HIGH, CRITICAL
- Controlled by `SECURITY_AUDIT_ENABLED` environment variable
- Centralized collection via CloudWatch; emits custom security metrics off the request path (see CloudWatch Integration)
- Compliance-relevant records (`SECURITY_AUDIT:` marker) forwarded to long-retention S3 storage (see Audit Log Retention)

### CloudWatch Integration

- Log groups for ECS tasks with configurable retention (default 30 days)
- Custom security metrics emitted off the request path to the `RoboSystems/Security/{environment}` namespace (`robosystems/security/audit_logger.py`)
- Alarms (actions wired to the `robosystems-{environment}-security-alerts` SNS topic when an alert email is configured):
  - `FailedAdminAuthAlarm`: `FailedAdminAuth` > 5 in 5 minutes
  - `AuthFailureSpikeAlarm`: `AuthFailure` > 50 in 5 minutes
  - `InjectionAttemptAlarm`: `InjectionAttempt` > 0
  - `PrivilegeEscalationAlarm`: `PrivilegeEscalationAttempt` > 0
  - `AuthorizationDeniedAlarm`: `AuthorizationDenied` > 20 in 5 minutes
- Optional Container Insights for deeper metrics

### Managed Monitoring

**Implementation:** `cloudformation/prometheus.yaml`, `cloudformation/grafana.yaml`

- Amazon Managed Prometheus for metrics collection
- Amazon Managed Grafana with CloudWatch, Prometheus, and Athena data sources
- SSO authentication via AWS Identity Center

### SNS Alerting

- Infrastructure alert topics per component (API security, PostgreSQL, Valkey, OpenSearch, graph writer tiers, graph infrastructure and volumes, shared replicas, Dagster, worker)
- Email subscriptions for each topic
- Secrets rotation notifications

## Compliance Infrastructure

Optional features disabled by default to minimize costs. Configured as CloudFormation parameters and deployed via their respective stacks.

### AWS CloudTrail

**Implementation:** `cloudformation/cloudtrail.yaml`

| Parameter | Description | Default |
|-----------|-------------|---------|
| `EnableCloudTrail` | Enable/disable CloudTrail | `false` |
| `LogRetentionDays` | Days to retain logs | `90` |
| `DataEventsEnabled` | Enable S3 data events logging | `false` |

- Multi-region trail with log file validation
- S3 storage with AES256 encryption, Intelligent-Tiering, and Object Lock (governance mode, 400 days)
- Automatic lifecycle rules for retention enforcement
- SSM Session Manager command logging: the `SSM-SessionManagerRunShell` document streams bastion session commands to the `/robosystems/ssm-sessions` CloudWatch log group (400-day retention); deployed with the trail, gated on `EnableCloudTrail`
- Tagged as SOC2-Compliance

### VPC Flow Logs

**Implementation:** `cloudformation/vpc.yaml` (conditional resource)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `EnableVPCFlowLogs` | Enable/disable VPC Flow Logs | `false` |
| `FlowLogsRetentionDays` | Days to retain logs | `90` |
| `FlowLogsTrafficType` | Traffic to capture (ALL/ACCEPT/REJECT) | `REJECT` |

- S3 storage with enhanced log format and 10-minute aggregation
- AES256 encryption at rest
- Tagged as SOC2-Compliance

### Security Baseline (Detective Controls)

**Implementation:** `cloudformation/security.yaml`

| Parameter | Description | Default |
|-----------|-------------|---------|
| `SECURITY_ENABLED` (GitHub variable) | Enable/disable the baseline stack | `false` |
| `EnableGuardDuty` | GuardDuty threat detection | `true` |
| `EnableSecurityHub` | Security Hub + AWS FSBP standard | `true` |
| `EnableCISStandard` | Also enable the CIS Foundations benchmark | `false` |
| `EnableAccessAnalyzer` | IAM Access Analyzer (account scope) | `true` |
| `EnableConfig` (`SECURITY_CONFIG_ENABLED` GitHub variable) | AWS Config recorder | `false` |

- Account-global shared stack (`RoboSystemsSecurity`), deployed by the gated `security` job in `deploy-vpc.yml` (sibling to the CloudTrail job)
- GuardDuty, Security Hub (FSBP), and Access Analyzer enable together via `SECURITY_ENABLED`; AWS Config is gated separately (`SECURITY_CONFIG_ENABLED`) as the cost outlier
- Amazon Inspector v2 is enabled via an idempotent `inspector2:enable` step in the deploy job (no native single-account CloudFormation resource)
- AWS Config records to a dedicated versioned, AES256-encrypted, retained S3 bucket
- Deploy-role IAM grants for these services live in `cloudformation/bootstrap-oidc.yaml` (re-run `just bootstrap` before enabling)
- Tagged as SOC2-Compliance

### Audit Log Retention

**Implementation:** `cloudformation/audit.yaml`

| Parameter | Description | Default |
|-----------|-------------|---------|
| `AUDIT_ENABLED_{PROD,STAGING}` (GitHub variable) | Enable/disable the retention pipeline | `false` |
| `AUDIT_RETENTION_DAYS` (`RetentionDays`) | Days to retain audit records in S3 | `400` |

- CloudWatch Logs subscription filter on `/robosystems/{environment}/api` forwards only compliance records (`SECURITY_AUDIT:` marker and structured operation-audit entries) to Kinesis Data Firehose → a dedicated S3 bucket
- Bucket is versioned, AES256-encrypted, public-access-blocked, Object-Locked (governance mode, 400 days), with ~13-month retention (`RetentionDays`)
- Preserves security evidence beyond the short operational-log retention; entirely log-side (no request-path impact)
- Tagged as SOC2-Compliance

### Validation Commands

```bash
# CloudTrail
aws cloudtrail get-trail-status --name robosystems-prod

# VPC Flow Logs
aws ec2 describe-flow-logs --filters "Name=resource-type,Values=VPC"

# WAF
aws wafv2 list-web-acls --scope REGIONAL

# Secrets
aws secretsmanager list-secrets --filters Key=name,Values=robosystems

# S3 encryption
aws s3api get-bucket-encryption --bucket robosystems-deployment-prod

# Security baseline
aws guardduty list-detectors
aws securityhub get-enabled-standards
aws configservice describe-configuration-recorders
aws accessanalyzer list-analyzers
aws inspector2 batch-get-account-status

# Audit log retention + SSM session logs
aws logs describe-subscription-filters --log-group-name /robosystems/prod/api
aws logs describe-log-groups --log-group-name-prefix /robosystems/ssm-sessions
```

### Log Locations

- CloudTrail: `s3://robosystems-cloudtrail-{environment}-{account-id}`
- VPC Flow Logs: `s3://robosystems-vpc-flow-logs-{environment}-{account-id}`
- AWS Config: `s3://robosystems-config-{account-id}`
- Audit retention: `s3://robosystems-audit-{environment}-{account-id}`
- SSM sessions: CloudWatch `/robosystems/ssm-sessions`
- Application: CloudWatch `/robosystems/{environment}/{api,worker,dagster,graph-api,bastion-host}`

## Startup Validation

**Implementation:** `robosystems/config/validation.py`

Production environment enforces at startup:
- `DATABASE_URL` must be set
- `JWT_SECRET_KEY` must be 32+ characters (and not contain "development")
- `VALKEY_URL` must be set
- `AWS_REGION` must be set
- `CONNECTION_CREDENTIALS_KEY` must be set
- Stripe keys validated when `BILLING_ENABLED=true`

## Incident Response

### Contact

- Security Team: security@robosystems.ai
- Administrative Team: admin@robosystems.ai
- Trust Center: https://trust.robosystems.ai

### Automated Response

- IP blocking for repeated authentication failures (threat level escalation)
- Rate limiting enforcement with tier-aware thresholds
- Load shedding under system pressure
- CloudWatch alarm on failed admin authentication

## Third-Party Security

- AWS shared responsibility model (AWS manages physical/infrastructure security)
- All third-party APIs use encrypted connections
- OAuth 2.0 for QuickBooks integration
- Read-only access for SEC EDGAR data


## Security Testing

- CloudFormation template validation in CI/CD
- Security-focused test markers (`@pytest.mark.security`)
- Black-box tenant-isolation harness (`@pytest.mark.isolation`), run against a deployed environment
- Code quality and security linting via Ruff, including the bandit (`S`) rule set
- Security-focused pull request reviews
