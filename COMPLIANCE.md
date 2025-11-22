# SOC 2 Compliance Configuration

This document outlines the SOC 2 compliance features available in the RoboSystems infrastructure and how to enable them.

## Overview

RoboSystems includes built-in security controls and optional SOC 2 compliance features that can be enabled through GitHub Actions variables. Most compliance features are disabled by default to minimize costs.

## Built-in Security Controls

**Authentication & Authorization:**

- JWT-based authentication with configurable expiration
- API key management with automatic rotation
- Role-based access control with tenant isolation
- Progressive authentication delays for failed attempts

**Data Protection:**

- TLS 1.3 for all API communications
- Database encryption at rest (PostgreSQL, LadybugDB)
- AWS Secrets Manager for credential management
- Multi-tenant database isolation

**Audit Logging:**

- Comprehensive security event logging
- Structured JSON format with correlation IDs
- Authentication and authorization event tracking

**Network Security:**

- VPC with private/public subnet segregation
- Security groups with least-privilege access
- Application Load Balancer with health checks

## Optional Compliance Features

### 1. AWS CloudTrail (Audit Logging)

CloudTrail provides audit logging of all AWS API calls.

**Configuration Variables:**

- `CLOUDTRAIL_ENABLED`: Enable/disable CloudTrail ("true"/"false", default: "true")
- `CLOUDTRAIL_LOG_RETENTION_DAYS`: Days to retain logs (default: 90)
- `CLOUDTRAIL_DATA_EVENTS_ENABLED`: Enable S3 data events logging ("true"/"false", default: "false")

### 2. VPC Flow Logs (Network Monitoring)

VPC Flow Logs capture IP traffic information in your VPC.

**Configuration Variables:**

- `VPC_FLOW_LOGS_ENABLED`: Enable/disable VPC Flow Logs ("true"/"false", default: "true")
- `VPC_FLOW_LOGS_RETENTION_DAYS`: Days to retain logs (default: 30)
- `VPC_FLOW_LOGS_TRAFFIC_TYPE`: Traffic to capture ("ALL", "ACCEPT", "REJECT", default: "REJECT")

### 3. AWS WAF (Web Application Firewall)

WAF protects web applications from common exploits.

**Configuration Variables:**

- `WAF_ENABLED_PROD`: Enable/disable WAF protection ("true"/"false", default: "true")
- `WAF_GEO_BLOCKING_ENABLED`: Enable geographic restrictions ("true"/"false", default: "false")
- `WAF_AWS_MANAGED_RULES_ENABLED`: Enable AWS managed rule sets ("true"/"false", default: "true")
- `WAF_RATE_LIMIT_PER_IP`: Rate limit per IP (requests per 5 minutes, default: 10000)
- `ADMIN_ALLOWED_CIDRS`: Admin API and WAF allowlist - IPs that can access admin endpoints and bypass all WAF rules

### 4. Enhanced Monitoring

**Configuration Variables:**

- `OBSERVABILITY_ENABLED_PROD`: Enable Prometheus/Grafana monitoring ("true"/"false")
- `DATABASE_MULTI_AZ_ENABLED_PROD`: Enable multi-AZ RDS deployment ("true"/"false", default: "false")
- `AWS_SNS_ALERT_EMAIL`: Email for system alerts

## Quick Setup

### Production Environment

```bash
# Enable core compliance features
gh variable set CLOUDTRAIL_ENABLED --body "true"
gh variable set VPC_FLOW_LOGS_ENABLED --body "true"
gh variable set WAF_ENABLED_PROD --body "true"
gh variable set OBSERVABILITY_ENABLED_PROD --body "true"
gh variable set AWS_SNS_ALERT_EMAIL --body "email@example.com"
```

## Deployment

After configuring variables:

```bash
# Deploy full infrastructure
gh workflow run prod.yml
```

## Monitoring

**Log Locations:**

- CloudTrail Logs: S3 bucket `robosystems-cloudtrail-{environment}-{account-id}`
- VPC Flow Logs: S3 bucket `robosystems-vpc-flow-logs-{environment}-{account-id}`
- Application Logs: CloudWatch Logs `/aws/ecs/{service-name}`

**Dashboards:**

- Grafana: Authentication metrics, system performance
- CloudWatch: AWS resource monitoring and alerting

## Validation

Check compliance features are working:

```bash
# Verify CloudTrail
aws cloudtrail get-trail-status --name robosystems-prod

# Verify VPC Flow Logs
aws ec2 describe-flow-logs --filters "Name=resource-type,Values=VPC"

# Verify WAF
aws wafv2 list-web-acls --scope REGIONAL
```

## Additional Documentation

Security policies and detailed procedures are documented separately. This document covers the technical configuration of compliance features only.
