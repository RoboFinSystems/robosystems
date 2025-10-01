# RoboSystems Security Implementation

This document describes the security features implemented in the RoboSystems platform.

## Overview

RoboSystems implements security controls at the infrastructure, application, and data levels. All security features described here are currently implemented and operational.

## Authentication and Access Control

### JWT Authentication

**Implementation:** `robosystems/middleware/auth/jwt.py`

- Configurable token expiration (default: 24 hours)
- Secure token generation and validation
- Automatic token refresh mechanisms

### API Key Management

**Implementation:** `robosystems/middleware/auth/dependencies.py`

- Per-tenant API key generation
- Automatic rotation (configurable, default: 90 days)
- Usage tracking and rate limiting

### Authentication Protection

**Implementation:** `robosystems/security/auth_protection.py`

- Progressive delays for failed authentication attempts
- IP-based threat assessment and blocking
- Automatic threat level escalation

**Features:**

- Failed attempt tracking with increasing delays
- IP blocking for repeated failures
- Threat level assessment (Low, Medium, High, Critical)

### Multi-Tenant Access Control

- Role-based access control (Read, Write, Admin)
- Complete tenant isolation via separate Kuzu databases
- API endpoints scoped by graph_id parameter

## Data Security

### Encryption at Rest

**Database Encryption:**

- PostgreSQL: AES-256 encryption via AWS RDS
- Kuzu: EBS volume encryption
- All backups encrypted

**File Storage:**

- S3 server-side encryption with AWS KMS
- Encrypted object versioning

### Encryption in Transit

- TLS 1.3 for all external API communications
- SSL/TLS required for all database connections
- Certificate management via AWS Certificate Manager

### Secrets Management

**Implementation:** `robosystems/config/secrets_manager.py`

- AWS Secrets Manager integration
- Automatic credential rotation
- Environment-specific secret isolation

**Managed Secrets:**

- Database credentials
- API keys and JWT secrets
- Third-party service credentials

## Infrastructure Security

### Network Security

**VPC Architecture:**

- Private/public subnet segmentation
- Security groups with least-privilege access
- No direct internet access to application/data tiers

**Load Balancing:**

- Application Load Balancer with health checks
- SSL termination at load balancer
- Multi-AZ deployment for high availability

### Web Application Firewall (WAF)

**Implementation:** `cloudformation/waf.yaml`

**Protection Rules:**

1. IP allowlist for trusted sources
2. Rate limiting (default: 10,000 requests per 5 minutes per IP)
3. AWS managed rules for common attack patterns
4. Optional geographic blocking

### Infrastructure as Code

- CloudFormation templates for all infrastructure
- Automated template validation in CI/CD
- Version-controlled infrastructure changes

## Application Security

### Input Validation

**Implementation:** Pydantic models throughout application

- Type checking and format validation for all API inputs
- Schema validation at API boundaries
- File upload validation and size limits

### Query Security

**Implementation:** `robosystems/security/cypher_analyzer.py`

- Cypher query analysis before execution
- Prevention of unauthorized write operations
- Parameter binding to prevent injection attacks

### Rate Limiting

**Implementation:** `robosystems/middleware/rate_limits/rate_limiting.py`

- Distributed rate limiting using Valkey
- Configurable limits per API key
- Sliding window algorithm for accurate limiting

## Security Monitoring

### Audit Logging

**Implementation:** `robosystems/security/audit_logger.py`

**Logged Events:**

- Authentication success/failure
- Authorization violations
- Administrative actions
- Security incidents

**Log Format:**

- Structured JSON with correlation IDs
- Centralized collection via CloudWatch
- Real-time security event tracking

### Monitoring Integration

- CloudWatch integration for metrics and alerting
- Grafana dashboards for security metrics
- SNS alerting for critical events

## Incident Response

### Security Contact Information

- Security Team: security@robosystems.ai
- Administrative Team: admin@robosystems.ai

### Automated Response

- IP blocking for repeated authentication failures
- Rate limiting enforcement
- Automatic threat level escalation

## Third-Party Security

### AWS Shared Responsibility

- AWS manages physical security and infrastructure
- RoboSystems manages application, data, and access controls

### External Integrations

- All third-party APIs use encrypted connections
- OAuth 2.0 for QuickBooks integration
- Read-only access for SEC EDGAR data

## Security Configuration

### Core Security Variables

```bash
# Authentication
JWT_ACCESS_TOKEN_EXPIRE_HOURS="24"
KUZU_API_KEY_ROTATION_DAYS="90"

# Audit logging
SECURITY_AUDIT_ENABLED="true"

# Database security
DATABASE_SECRETS_ROTATION_DAYS="90"
VALKEY_ENCRYPTION_ENABLED_PROD="true"
VALKEY_SECRET_ROTATION_ENABLED_PROD="true"

# Infrastructure security
WAF_ENABLED_PROD="true"
WAF_AWS_MANAGED_RULES_ENABLED="true"
```

### Validation Commands

```bash
# Check secret rotation status
aws secretsmanager list-secrets --filters Key=name,Values=robosystems

# Verify encryption status
aws s3api get-bucket-encryption --bucket robosystems-prod-deployment

# Check WAF status
aws wafv2 list-web-acls --scope REGIONAL
```

## Security Testing

### Automated Security Checks

- CloudFormation template validation
- Dependency vulnerability scanning (when applicable)
- Code quality and security linting

### Manual Security Reviews

- Code review requirements for all changes
- Security-focused pull request reviews
- Regular access reviews

## Documentation

Security policies, incident response procedures, and compliance documentation are maintained separately. This document covers the technical implementation of security controls only.

For security issues or questions, contact: security@robosystems.ai
