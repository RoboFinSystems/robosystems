High-performance REST API for Kuzu graph database operations. Provides multi-tenant database management, query execution, data ingestion, and backup/restore capabilities with built-in monitoring and health checks.

## Core Features

- **Multi-Tenant Architecture**: Isolated database instances per graph with API key authentication
- **High Performance**: Connection pooling, async operations, and NDJSON streaming for large results
- **Data Ingestion**: Batch ingestion from Parquet/CSV files with S3 integration
- **Backup & Restore**: Full database backups with encryption support
- **Monitoring**: Health checks, metrics, and task tracking

## Infrastructure Configuration

### Node Types

#### Writer Nodes

Full read/write access nodes for user graph databases and shared repository management.

- **Port**: 8001
- **Access**: Full CRUD operations on assigned databases
- **Storage**: EBS volumes with automatic attachment/detachment
- **Registry**: DynamoDB-based tracking for instances, volumes, and graph assignments
- **Protection**: Instance termination protection when databases are allocated

#### Shared Master (Writer)

Special writer node hosting shared repository databases (SEC, industry data).

- **Port**: 8001
- **Databases**: Hosts `sec` and other shared repositories
- **Access**: Admin-only write access, read access via subscriptions
- **Backups**: Automated EBS snapshots for replica synchronization

#### Shared Replica Nodes

Read-only nodes for shared repository queries.

- **Port**: 8002
- **Access**: Read-only operations on shared repositories
- **Sync**: Restored from shared master EBS snapshots
- **Scaling**: Auto-scaling based on query load

### Service Tiers

#### Standard Tier

Entry-level configuration for individual users and small teams.

- **Instance Type**: r7g.xlarge
- **Memory**: 2GB per database
- **Databases per Node**: 10 maximum
- **Query Chunk Size**: 1,000 rows per batch
- **Connection Pool**: 10 connections per database
- **Use Case**: Development, testing, small-scale production

#### Enterprise Tier

Isolated resources for business-critical workloads.

- **Instance Type**: r7g.large
- **Memory**: 14GB total (dedicated instance)
- **Databases per Node**: 1 (isolated)
- **Subgraphs per Node**: 10 subgraphs
- **Query Chunk Size**: 5,000 rows per batch
- **Connection Pool**: Enhanced connection limits
- **Use Case**: Production workloads requiring isolation

#### Premium Tier

Maximum performance configuration for demanding applications.

- **Instance Type**: r7g.xlarge
- **Memory**: 28GB total (dedicated instance)
- **Databases per Node**: 1 (isolated)
- **Subgraphs per Node**: Unlimited subgraphs
- **Query Chunk Size**: 10,000 rows per batch
- **Connection Pool**: Maximum connection capacity
- **Use Case**: High-performance analytics, large-scale operations

## Authentication

API key authentication required for all database operations:

```
X-Kuzu-API-Key: kuzu*
```
