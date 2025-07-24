# OpenSearch Serverless Cross-Account Migration Tool

This tool facilitates the migration of data between OpenSearch Serverless collections, either within the same AWS account or across different AWS accounts.

## Code Structure

The migration process has been split into multiple components:

1. **base_migration.py**: Contains the `BaseMigration` class with shared functionality used by all migration components.

2. **source_to_s3.py**: Handles the first step of migration - extracting data from source OpenSearch to S3.
   - Creates and manages source ingestion pipelines
   - Monitors extraction progress using CloudWatch metrics
   - Handles cleanup of source resources

3. **s3_to_target.py**: Handles the second step of migration - loading data from S3 to target OpenSearch.
   - Creates and manages target ingestion pipelines
   - Monitors loading progress
   - Handles cleanup of target resources

4. **migration_orchestrator.py**: Orchestrates the entire migration process.
   - Coordinates the source-to-S3 and S3-to-target migrations
   - Manages concurrent index migrations
   - Provides the main entry point for the tool

## Configuration

The tool requires a JSON configuration file with the following structure:

```json
{
  "source": {
    "host": "https://source-opensearch-endpoint.amazonaws.com",
    "role": "arn:aws:iam::source-account-id:role/source-role-name",
    "region": "us-east-1",
    "account_id": "source-account-id",
    "network_policy_name": "source-network-policy"
  },
  "target": {
    "host": "https://target-opensearch-endpoint.amazonaws.com",
    "role": "arn:aws:iam::target-account-id:role/target-role-name",
    "region": "us-east-1",
    "account_id": "target-account-id",
    "network_policy_name": "target-network-policy"
  },
  "shared": {
    "s3_bucket_name": "migration-bucket-name",
    "s3_bucket_region": "us-east-1",
    "max_concurrent": 3,
    "verbose": false
  }
}
```

## Usage

Run the migration tool using the following command:

```bash
python migration_orchestrator.py --config path/to/config.json [--verbose]
```

Options:
- `--config`, `-c`: Path to the JSON configuration file (required)
- `--verbose`, `-v`: Enable verbose logging (optional)

## Migration Process

1. The tool first identifies all indices in the source OpenSearch collection.
2. For each index (with configurable concurrency):
   - Creates a source pipeline to extract data to S3
   - Monitors extraction progress
   - Creates a destination pipeline to load data from S3 to target
   - Monitors loading progress
   - Validates document counts
   - Cleans up resources
   - Updates the migration tracker

## Requirements

- Python 3.7+
- boto3
- requests
- requests_aws4auth

## IAM Permissions

The IAM roles specified in the configuration need the following permissions:

- Source role:
  - OpenSearch Serverless read permissions
  - S3 write permissions
  - CloudWatch Logs and Metrics permissions
  - OSIS (OpenSearch Ingestion Service) permissions

- Target role:
  - OpenSearch Serverless read/write permissions
  - S3 read permissions
  - CloudWatch Logs and Metrics permissions
  - OSIS permissions