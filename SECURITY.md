# RoboSystems Security

Security controls implemented at the infrastructure, application, and data levels.

Live compliance posture and audit artifacts are published in the [RoboSystems Trust Center](https://trust.robosystems.ai).

## Authentication and Access Control

### JWT Authentication

**Implementation:** `robosystems/middleware/auth/jwt.py`

- 30-minute access token expiration (configurable via `JWT_EXPIRY_HOURS`)
- JTI-based token revocation tracked in Valkey with TTL
- Automatic token refresh at `/v1/auth/refresh` with 5-second grace period for in-flight requests
- Issuer and audience claim validation
- Fails closed on Valkey errors (treats token as revoked)

### Device Fingerprinting

**Implementation:** `robosystems/security/device_fingerprinting.py`

- Browser binding via user-agent and Sec-CH-UA headers (SHA256 hash)
- Intentionally excludes IP address (VPN/mobile/load balancer changes)
- Detects suspicious changes in browser client hints

### API Key Management

**Implementation:** `robosystems/models/core/user/user_api_key.py`

- Cryptographically secure generation (`rfs` prefix + 64 hex characters)
- Bcrypt hashing with cost factor 12 for storage
- Optional per-key expiration (`expires_at`)
- Usage tracking via `last_used_at` timestamp
- Monthly rotation via GitHub Actions (`secrets-rotation.yml`) using AWS Secrets Manager Lambda functions

### Authentication Protection

**Implementation:** `robosystems/security/auth_protection.py`

- Progressive delays on failed attempts (1s, 2s, 5s, 10s, 30s, 1m, 5m, 15m cap)
- IP-based threat assessment with four levels:
  - LOW: <5 failures
  - MEDIUM: 5+ failures (15-minute block)
  - HIGH: 10+ failures (1-hour block)
  - CRITICAL: 20+ failures (24-hour block)
- Blocks automatically expire; 24-hour sliding window per IP
- Stored in Valkey with SHA256-hashed keys; graceful degradation on cache failure

### CAPTCHA Verification

**Implementation:** `robosystems/security/captcha.py`

- Cloudflare Turnstile server-side token verification
- Configurable via `CAPTCHA_ENABLED` environment variable
- Applied to registration and authentication endpoints

### Password Security

**Implementation:** `robosystems/security/password.py`

- Bcrypt hashing with 14 rounds
- Score-based strength validation (min 60/100 to pass)
- Pattern detection: sequential chars, repeated chars, common passwords
- Requirements: 12+ chars, uppercase, lowercase, digit, special character, 8+ unique characters

### Multi-Tenant Access Control

**Implementation:** `robosystems/models/core/graph/graph_user.py`

- Roles: admin (full control), member (read/write, default), viewer (read-only)
- Complete tenant isolation via separate LadybugDB databases
- Subgraph permissions inherited from parent graph
- All endpoints scoped by `graph_id`

## Data Security

### Encryption at Rest

- PostgreSQL: AES-256 encryption via AWS RDS
- LadybugDB: EBS volume encryption
- S3: AES256 server-side encryption (SSE-S3) on all buckets
- Graph backups: SSE-AES256 on the stored object; served only over TLS through
  short-lived signed URLs. Not additionally encrypted at the application layer —
  a backup download is a usable `.lbug` database by design.

### Encryption in Transit

- TLS 1.2+ enforced on CloudFront distributions
- SSL/TLS required for all database connections (`rds.force_ssl: 1`)
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
- TTL-based caching (1-hour default) with two-level LRU + timestamp cache
- 40+ secrets mapped (JWT, database, S3, Stripe, Intuit, etc.)
- Graceful fallback to environment variables in development

### Secrets Rotation

**Implementation:** `.github/workflows/secrets-rotation.yml`

- Monthly schedule via GitHub Actions (staging: 1st of month, prod: 2nd of month)
- Lambda-based rotation functions for each secret type
- Rotated secrets: PostgreSQL password, Valkey auth token, Graph API key, Admin API key
- Pre-rotation cleanup for stuck AWSPENDING versions
- Post-rotation verification with timeout handling (5 min for Postgres/API keys, 12 min for Valkey)
- Automatic service refresh after successful rotation
- SNS email notifications on completion
- Controlled by `SECRETS_ROTATION_ENABLED_STAGING` and `SECRETS_ROTATION_ENABLED_PROD` GitHub variables (disabled by default)

## Application Security

### Input Validation

**Implementation:** `robosystems/security/input_validation.py` + Pydantic models

- HTML escape and dangerous character removal (`<>"\'\0\r\n`)
- Email, username, UUID, URL, and SQL identifier validation
- Password strength validation with entropy scoring
- Recursive sanitization of nested dicts/lists
- Pydantic `EmailStr`, `min_length`/`max_length` constraints, and custom validators at API boundaries

### Query Security

**Implementation:** `robosystems/security/cypher_analyzer.py`

- AST-based Cypher query analysis (comment/string removal before keyword detection)
- Write operation detection (44 keywords: CREATE, MERGE, SET, DELETE, etc.)
- Operation classification: READ, WRITE, MIXED, BULK, ADMIN, SCHEMA_DDL
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
- Subscription-aware: tier-based multipliers (1.0x, 2.5x, 5.0x)
- Per-endpoint category limits (18 categories: auth, graph_read, graph_write, etc.)
- User identification: API key (SHA256 hash) > JWT (user_id) > IP (fallback)
- Response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`
- Sensitive auth endpoints: login (5/5min), register (3/hour), JWT refresh (20/min)

### Admission Control

**Implementation:** `robosystems/middleware/graph/admission_control.py`

- Memory, CPU, and queue pressure thresholds (tunable via SSM)
- Load shedding with configurable start/stop pressure thresholds
- Applied to both main API and Graph API (LadybugDB)

### File Upload Security

**Implementation:** `robosystems/routers/graphs/files/upload.py`

- Presigned S3 URLs for time-limited direct uploads (no API bottleneck)
- File format validation (parquet, csv, json)
- Per-file size limits and tier-based storage caps
- Row count validation for parquet files

## Infrastructure Security

### Network Security

**Implementation:** `cloudformation/vpc.yaml`

- Private/public subnet segmentation across 2-5 availability zones
- NAT Gateway for private subnet outbound access
- Security groups with least-privilege access
- VPC endpoints for S3, DynamoDB (gateway, free), Secrets Manager, ECR (interface)
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
- Multi-AZ deployment support (optional, off by default)

### CI/CD Security (OIDC)

**Implementation:** `cloudformation/bootstrap-oidc.yaml`

- GitHub OIDC federation (no long-term AWS credentials)
- Federated role restricted to `main` branch, `release/*` branches, and `v*` tags
- Scoped to specific repositories (robosystems, robosystems-app, roboledger-app, roboinvestor-app)
- 1-hour maximum session duration
- Permission scoping: ECR limited to `robosystems*`, S3 limited to `robosystems-*`

### Infrastructure as Code

- CloudFormation templates for all infrastructure
- Automated template validation in CI/CD
- Version-controlled infrastructure changes

## Security Monitoring

### Audit Logging

**Implementation:** `robosystems/security/audit_logger.py`

- Structured JSON logging with 15+ event types:
  - Authentication: success, failure, token expired/invalid
  - Authorization: denied, privilege escalation attempt
  - Security: injection attempt, rate limit exceeded, suspicious activity
  - Operations: data import, timeout, financial transaction
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
- Optional Container Insights for deeper metrics

### Managed Monitoring

**Implementation:** `cloudformation/prometheus.yaml`, `cloudformation/grafana.yaml`

- Amazon Managed Prometheus for metrics collection
- Amazon Managed Grafana with CloudWatch, Prometheus, and Athena data sources
- SSO authentication via AWS Identity Center

### SNS Alerting

- Infrastructure alert topics per component (PostgreSQL, graph, shared replicas)
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
- S3 storage with AES256 encryption and Intelligent-Tiering
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
- Bucket is versioned, AES256-encrypted, public-access-blocked, with ~13-month retention (`RetentionDays`)
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
- Application: CloudWatch `/aws/ecs/{service-name}`

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
- Code quality and security linting via Ruff
- Security-focused pull request reviews
