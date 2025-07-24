import boto3
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from requests_aws4auth import AWS4Auth
from typing import Dict, List, Tuple, Optional, Any, Union
from botocore.exceptions import ClientError
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseMigration:
    """Base class for OpenSearch Serverless migration operations."""
    
    def __init__(self, config_file: str):
        """
        Initialize the OpenSearch migration base class.
        
        Args:
            config_file: Path to the JSON configuration file
        """
        # Load configuration from JSON file
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Validate required configuration fields
        required_fields = {
            'source': ['host', 'role', 'region', 'account_id', 'network_policy_name'],
            'target': ['host', 'role', 'region', 'account_id', 'network_policy_name'],
            'shared': ['s3_bucket_name', 's3_bucket_region']
        }
        
        for section, fields in required_fields.items():
            if section not in config:
                raise ValueError(f"Missing required section '{section}' in configuration")
            
            for field in fields:
                if field not in config[section]:
                    raise ValueError(f"Missing required field '{field}' in '{section}' section")
        
        # Source account configuration
        self.source_host = config['source']['host']
        self.source_role = config['source']['role']
        self.source_region = config['source']['region']
        self.source_account_id = config['source']['account_id']
        self.source_vpc_config = config['source'].get('vpc_config', {})
        self.source_network_policy_name = config['source']['network_policy_name']
        
        # Target account configuration
        self.target_host = config['target']['host']
        self.target_role = config['target']['role']
        self.target_region = config['target']['region']
        self.target_account_id = config['target']['account_id']
        self.target_network_policy_name = config['target']['network_policy_name']
        
        # Log configuration details
        logger.info(f"Source account: {self.source_account_id}, region: {self.source_region}")
        logger.info(f"Target account: {self.target_account_id}, region: {self.target_region}")
        logger.info(f"Source role: {self.source_role}")
        logger.info(f"Target role: {self.target_role}")
        
        # Shared configuration
        self.s3_bucket_name = config['shared']['s3_bucket_name']
        self.s3_bucket_region = config['shared']['s3_bucket_region']
        self.max_concurrent = config['shared'].get('max_concurrent', 3)
        self.migration_tracker_file = config['shared'].get('migration_tracker_file', "migration_tracker.json")
        self.src_pipeline_logs = config['shared'].get('source_cloud_watch_logs', '/aws/vendedlogs/ossmigration/srcpipelinelogs')
        self.target_pipeline_logs = config['shared'].get('target_cloud_watch_logs', '/aws/vendedlogs/ossmigration/targetpipelinelogs')
        
        # Set log level based on verbosity
        if config['shared'].get('verbose', False):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize source account AWS clients
        self.source_session = boto3.Session(region_name=self.source_region)
        self.source_credentials = self.source_session.get_credentials()
        
        self.source_pipeline_client = self.source_session.client('osis', region_name=self.source_region)
        self.source_logs_client = self.source_session.client('logs', region_name=self.source_region)
        self.source_cloudwatch_client = self.source_session.client('cloudwatch', region_name=self.source_region)
        
        # Initialize target account AWS clients
        if self.source_account_id != self.target_account_id:
            # Always assume the target role for cross-account operations
            logger.info("Cross-account operation detected. Assuming target role...")
            self.target_session = self._create_target_session()
            # Initialize credential tracking
            self.last_credential_refresh = datetime.now(timezone.utc)
            self.credential_refresh_interval = timedelta(minutes=14)  # Refresh before the 1-hour expiry
            self.target_credentials_valid = True  # Credentials are valid after successful creation
        else:
            # Same account, use source session
            self.target_session = self.source_session
            self.target_credentials_valid = True
            self.last_credential_refresh = None  # No need to track for same account
            
        self.target_pipeline_client = self.target_session.client('osis', region_name=self.target_region)
        self.target_logs_client = self.target_session.client('logs', region_name=self.target_region)
        self.target_cloudwatch_client = self.target_session.client('cloudwatch', region_name=self.target_region)
        
        # S3 client using target region since the bucket is in the target region
        self.s3_client = self.target_session.client('s3', region_name=self.s3_bucket_region)
        
        # Initialize AWS4Auth for OpenSearch API requests
        self.source_awsauth = AWS4Auth(
            self.source_credentials.access_key,
            self.source_credentials.secret_key,
            self.source_region,
            'aoss',
            session_token=self.source_credentials.token
        )
        
        # Create AWS4Auth for target account OpenSearch API requests
        target_credentials = self.target_session.get_credentials()
        self.target_awsauth = AWS4Auth(
            target_credentials.access_key,
            target_credentials.secret_key,
            self.target_region,
            'aoss',
            session_token=target_credentials.token
        )
        
        # Initialize migration tracker
        self._init_migration_tracker()
        
    def _check_target_credentials(self) -> bool:
        """
        Check if current credentials are valid for the target account.
        
        Returns:
            bool: True if credentials are valid, False otherwise
        """
        try:
            # Try to list OpenSearch collections in target account
            osis_client = self.source_session.client('opensearchserverless', region_name=self.target_region)
            osis_client.list_collections()
            logger.info("Current credentials are valid for target account")
            return True
        except ClientError as e:
            logger.info(f"Current credentials not valid for target account: {e}")
            return False
    
    def _create_target_session(self) -> boto3.Session:
        """
        Create a boto3 session for the target account by assuming a role.
        
        Returns:
            boto3.Session: Session for the target account
        """
        try:
            sts_client = self.source_session.client('sts')
            logger.info("Current identity:")
            logger.info(json.dumps(sts_client.get_caller_identity(), default=str, indent=2))
            assumed_role = sts_client.assume_role(
                RoleArn=self.target_role,
                RoleSessionName="OpenSearchMigrationSession1",
                DurationSeconds=900
            )
            logger.info(assumed_role)
            logger.info('Getting Credentials')
            credentials = assumed_role['Credentials']
            
            logger.info("target Session")
            target_session = boto3.Session(
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=self.target_region
            )
            
            logger.info(f"Successfully assumed role in target account: {self.target_account_id}")
            return target_session
            
        except ClientError as e:
            logger.error(f"Failed to assume role in target account: {e}")
            raise
            
    def refresh_target_credentials_if_needed(self) -> None:
        """
        Check if target credentials need to be refreshed and refresh them if needed.
        """
        # Skip if we're in same-account mode
        if self.source_account_id == self.target_account_id:
            return
            
        # Check if credentials are marked as invalid OR if it's time to refresh
        current_time = datetime.now(timezone.utc)
        should_refresh = (
            not self.target_credentials_valid or 
            (self.last_credential_refresh and 
             current_time - self.last_credential_refresh >= self.credential_refresh_interval)
        )
        
        if should_refresh:
            logger.info("Refreshing target account credentials...")
            
            # Create new session with refreshed credentials
            self.target_session = self._create_target_session()
            self.last_credential_refresh = current_time
            
            # Update all clients with the new session
            self.target_pipeline_client = self.target_session.client('osis', region_name=self.target_region)
            self.target_logs_client = self.target_session.client('logs', region_name=self.target_region)
            self.target_cloudwatch_client = self.target_session.client('cloudwatch', region_name=self.target_region)
            
            # Update S3 client if it's in the target region
            if self.s3_bucket_region == self.target_region:
                self.s3_client = self.target_session.client('s3', region_name=self.s3_bucket_region)
            
            # Update AWS4Auth for target account
            target_credentials = self.target_session.get_credentials()
            self.target_awsauth = AWS4Auth(
                target_credentials.access_key,
                target_credentials.secret_key,
                self.target_region,
                'aoss',
                session_token=target_credentials.token
            )
            
            logger.info("Target account credentials refreshed successfully")
            
    def _handle_credential_error(self, error: Exception, is_target: bool) -> bool:
        """
        Handle credential-related errors by refreshing credentials if needed.
        
        Args:
            error: The exception that occurred
            is_target: Whether the error occurred in the target account
            
        Returns:
            bool: True if credentials were refreshed, False otherwise
        """
        if is_target and self.source_account_id != self.target_account_id:
            error_str = str(error)
            if any(err in error_str for err in ['ExpiredToken', 'InvalidToken', 'AccessDenied']):
                logger.info(f"Credential error detected: {error_str}")
                # Mark credentials as invalid to force refresh
                self.target_credentials_valid = False
                self.refresh_target_credentials_if_needed()
                # Mark as valid again after successful refresh
                self.target_credentials_valid = True
                return True
        return False
    
    def _init_migration_tracker(self) -> None:
        """Initialize the migration tracker file if it doesn't exist."""
        if not os.path.exists(self.migration_tracker_file):
            with open(self.migration_tracker_file, 'w') as f:
                json.dump({"migration_status": []}, f, indent=4)
            logger.info(f"Created migration tracker file: {self.migration_tracker_file}")
            
    def get_indices(self, host: str, is_target: bool = False) -> List[str]:
        """
        Get list of indices from an OpenSearch Serverless endpoint.
        
        Args:
            host: OpenSearch Serverless endpoint URL
            is_target: Whether this is the target OpenSearch endpoint
            
        Returns:
            List of index names
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        url = f"{host}/_cat/indices?format=json"
        auth = self.target_awsauth if is_target else self.source_awsauth
        
        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            
            indices = [doc['index'] for doc in response.json()]
            logger.info(f"Found {len(indices)} indices in {'target' if is_target else 'source'} collection")
            return indices
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting indices from {'target' if is_target else 'source'}: {e}")
            
            # Try refreshing credentials and retry if it's a credential error
            if self._handle_credential_error(e, is_target):
                # Retry with refreshed credentials
                auth = self.target_awsauth if is_target else self.source_awsauth
                try:
                    response = requests.get(url, auth=auth)
                    response.raise_for_status()
                    indices = [doc['index'] for doc in response.json()]
                    logger.info(f"Found {len(indices)} indices in {'target' if is_target else 'source'} collection after credential refresh")
                    return indices
                except requests.exceptions.RequestException as retry_e:
                    logger.error(f"Error getting indices after credential refresh: {retry_e}")
            
            return []

    def get_index_doc_count(self, host: str, index_name: str, is_target: bool = False) -> int:
        """
        Get document count for a specific index.
        
        Args:
            host: OpenSearch Serverless endpoint URL
            index_name: Name of the index
            is_target: Whether this is the target OpenSearch endpoint
            
        Returns:
            Document count as integer
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        url = f"{host}/_cat/indices/{index_name}?format=json"
        auth = self.target_awsauth if is_target else self.source_awsauth
        
        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            return int(response.json()[0]['docs.count'])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting index document count: {e}")
            
            # Try refreshing credentials and retry if it's a credential error
            if self._handle_credential_error(e, is_target):
                # Retry with refreshed credentials
                auth = self.target_awsauth if is_target else self.source_awsauth
                try:
                    response = requests.get(url, auth=auth)
                    response.raise_for_status()
                    return int(response.json()[0]['docs.count'])
                except requests.exceptions.RequestException as retry_e:
                    logger.error(f"Error getting index document count after credential refresh: {retry_e}")
            
            return 0

    def create_log_group(self, log_group_name: str, is_target: bool = False) -> bool:
        """
        Create a CloudWatch log group in either source or target account.
        
        Args:
            log_group_name: Name of the log group to create
            is_target: Whether to create in target account
            
        Returns:
            True if log group was created or already exists, False otherwise
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        print('Creating logs')
        logs_client = self.target_logs_client if is_target else self.source_logs_client
        account_name = "target" if is_target else "source"
        
        try:
            logs_client.create_log_group(logGroupName=log_group_name)
            logger.info(f"Created log group in {account_name} account: {log_group_name}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                logger.info(f"Log group already exists in {account_name} account: {log_group_name}")
                return True
            else:
                logger.error(f"Error creating log group in {account_name} account: {e}")
                
                # Try refreshing credentials and retry if it's a credential error
                if self._handle_credential_error(e, is_target):
                    # Retry with refreshed credentials
                    logs_client = self.target_logs_client if is_target else self.source_logs_client
                    try:
                        logs_client.create_log_group(logGroupName=log_group_name)
                        logger.info(f"Created log group in {account_name} account after credential refresh: {log_group_name}")
                        return True
                    except ClientError as retry_e:
                        if retry_e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                            logger.info(f"Log group already exists in {account_name} account: {log_group_name}")
                            return True
                        else:
                            logger.error(f"Error creating log group after credential refresh: {retry_e}")
                
                return False
                
    def wait_for_pipeline_active(
        self, 
        pipeline_name: str, 
        is_target: bool = False,
        max_attempts: int = 30, 
        delay_seconds: int = 10
    ) -> bool:
        """
        Poll the pipeline status until it becomes ACTIVE or reaches max attempts.
        
        Args:
            pipeline_name: Name of the pipeline to monitor
            is_target: Whether this pipeline is in the target account
            max_attempts: Maximum number of polling attempts
            delay_seconds: Delay between polling attempts in seconds
            
        Returns:
            True if pipeline is active, False otherwise
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        pipeline_client = self.target_pipeline_client if is_target else self.source_pipeline_client
        account_name = "target" if is_target else "source"
        
        logger.info(f"Waiting for pipeline '{pipeline_name}' in {account_name} account to become active...")
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Refresh credentials periodically during long waits
                if is_target and self.source_account_id != self.target_account_id and attempt % 5 == 0:
                    self.refresh_target_credentials_if_needed()
                    pipeline_client = self.target_pipeline_client
                    
                response = pipeline_client.get_pipeline(PipelineName=pipeline_name)
                status = response['Pipeline'].get('Status')
                
                logger.info(f"Attempt {attempt}/{max_attempts}: Pipeline: {pipeline_name} in {account_name} account status: {status}")
                
                if status == 'ACTIVE':
                    logger.info(f"Pipeline '{pipeline_name}' in {account_name} account is now ACTIVE!")
                    return True
                elif status in ['FAILED', 'DELETED']:
                    logger.error(f"Pipeline in {account_name} account reached terminal state: {status}")
                    return False
                    
                # Wait before next check
                time.sleep(delay_seconds)
                
            except ClientError as e:
                logger.error(f"Error checking pipeline status in {account_name} account: {e}")
                
                # Try refreshing credentials and continue if it's a credential error
                if self._handle_credential_error(e, is_target):
                    pipeline_client = self.target_pipeline_client if is_target else self.source_pipeline_client
                    logger.info("Credentials refreshed, continuing pipeline status check")
                
                time.sleep(delay_seconds)
        
        logger.error(f"Timed out waiting for pipeline '{pipeline_name}' in {account_name} account to become active")
        return False
        
    def get_pipeline_metric_stats(
        self, 
        pipeline_name: str, 
        metric_name: str, 
        start_time: datetime, 
        end_time: datetime, 
        is_target: bool = False,
        period: int = 60
    ) -> int:
        """
        Get metric statistics from CloudWatch for an OpenSearch Ingestion Pipeline.

        Args:
            pipeline_name: Name of the OpenSearch Ingestion Pipeline
            metric_name: Name of the metric to query (e.g., 'recordsIn.count')
            start_time: Start time for the metric query
            end_time: End time for the metric query
            is_target: Whether this pipeline is in the target account
            period: Period in seconds for the metric aggregation

        Returns:
            Total sum of the metric values
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        cloudwatch_client = self.target_cloudwatch_client if is_target else self.source_cloudwatch_client
        account_name = "target" if is_target else "source"
        
        logger.info(f'Getting metrics for {pipeline_name} in {account_name} account - {metric_name}')
        logger.info(f'Time range: {start_time} to {end_time}')
        
        try:
            response = cloudwatch_client.get_metric_statistics(
                Namespace='AWS/OSIS',
                MetricName=metric_name,
                Dimensions=[
                    {
                        'Name': 'PipelineName',
                        'Value': pipeline_name
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=['Sum']
            )

            total_sum = 0
            for datapoint in response['Datapoints']:
                logger.debug(f"{datapoint['Timestamp']}: {datapoint['Sum']}")
                total_sum += datapoint['Sum']

            return int(total_sum)
            
        except ClientError as e:
            logger.error(f"Error getting metrics in {account_name} account: {e}")
            
            # Try refreshing credentials and retry if it's a credential error
            if self._handle_credential_error(e, is_target):
                # Retry with refreshed credentials
                cloudwatch_client = self.target_cloudwatch_client if is_target else self.source_cloudwatch_client
                try:
                    response = cloudwatch_client.get_metric_statistics(
                        Namespace='AWS/OSIS',
                        MetricName=metric_name,
                        Dimensions=[
                            {
                                'Name': 'PipelineName',
                                'Value': pipeline_name
                            }
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=period,
                        Statistics=['Sum']
                    )

                    total_sum = 0
                    for datapoint in response['Datapoints']:
                        logger.debug(f"{datapoint['Timestamp']}: {datapoint['Sum']}")
                        total_sum += datapoint['Sum']

                    return int(total_sum)
                except ClientError as retry_e:
                    logger.error(f"Error getting metrics after credential refresh: {retry_e}")
            
            return 0
            
    def get_migration_status(
        self,
        host: str,
        index_name: str, 
        pipeline_name: str, 
        metric_name: str, 
        start_time: datetime,
        is_target: bool = False,
        period: int = 60, 
        delay_seconds: int = 60,
        timeout_minutes: int = 120
    ) -> bool:
        """
        Monitor migration progress by comparing source document count with processed records.
        Continues checking until processed count equals source document count or timeout occurs.
        
        Args:
            host: Source OpenSearch endpoint URL
            index_name: Name of the index being migrated
            pipeline_name: Name of the pipeline to monitor
            metric_name: CloudWatch metric name to track
            start_time: Start time for metrics collection
            is_target: Whether this pipeline is in the target account
            period: Period in seconds for the metric aggregation
            delay_seconds: Delay between polling attempts in seconds
            timeout_minutes: Maximum time to wait in minutes before timing out
            
        Returns:
            True if migration completed successfully, False otherwise
        """
        src_docs_count = self.get_index_doc_count(host, index_name)
        if src_docs_count == 0:
            logger.warning(f"Source index {index_name} has 0 documents, nothing to migrate")
            return True
            
        start_check_time = datetime.now()
        timeout_delta = timedelta(minutes=timeout_minutes)
        check_count = 0
        account_name = "target" if is_target else "source"
        
        while True:
            check_count += 1
            # Check if we've exceeded the timeout
            if datetime.now() - start_check_time > timeout_delta:
                logger.error(f"Migration in {account_name} account timed out after {timeout_minutes} minutes")
                return False
                
            # Refresh credentials periodically during long monitoring
            if is_target and self.source_account_id != self.target_account_id and check_count % 5 == 0:
                self.refresh_target_credentials_if_needed()
                
            # Update end_time to current time for each check
            current_end_time = datetime.now(timezone.utc)
            
            # Get current metrics count
            processed_count = self.get_pipeline_metric_stats(
                pipeline_name, 
                metric_name, 
                start_time, 
                current_end_time,
                is_target,
                period
            )
            
            # Check if migration is complete
            if processed_count >= src_docs_count:
                logger.info(f"Source documents and processed records match in {account_name} account - {index_name}: {src_docs_count}")
                return True
                
            # Log progress
            logger.info(f"Check #{check_count} in {account_name} account: Source document count: {src_docs_count}, "
                       f"Processed records: {processed_count} - index: {index_name}")
            logger.info(f"Migration progress in {account_name} account: {processed_count}/{src_docs_count} "
                       f"({(processed_count/src_docs_count*100):.2f}%) - {index_name}")
            
            # Wait before next check
            time.sleep(delay_seconds)

    def delete_pipeline(self, pipeline_name: str, is_target: bool = False) -> Dict[str, Any]:
        """
        Delete an OpenSearch Ingestion Pipeline in either source or target account.
        
        Args:
            pipeline_name: Name of the pipeline to delete
            is_target: Whether this pipeline is in the target account
            
        Returns:
            Response from the delete_pipeline API call
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        pipeline_client = self.target_pipeline_client if is_target else self.source_pipeline_client
        account_name = "target" if is_target else "source"
        
        try:
            response = pipeline_client.delete_pipeline(PipelineName=pipeline_name)
            logger.info(f"Successfully deleted pipeline in {account_name} account: {pipeline_name}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting pipeline in {account_name} account: {e}")
            
            # Try refreshing credentials and retry if it's a credential error
            if self._handle_credential_error(e, is_target):
                # Retry with refreshed credentials
                pipeline_client = self.target_pipeline_client if is_target else self.source_pipeline_client
                try:
                    response = pipeline_client.delete_pipeline(PipelineName=pipeline_name)
                    logger.info(f"Successfully deleted pipeline in {account_name} account after credential refresh: {pipeline_name}")
                    return response
                except ClientError as retry_e:
                    logger.error(f"Error deleting pipeline after credential refresh: {retry_e}")
            
            raise

    def delete_log_group(self, log_group_name: str, is_target: bool = False) -> bool:
        """
        Delete a CloudWatch log group in either source or target account.

        Args:
            log_group_name: Name of the log group to delete
            is_target: Whether this log group is in the target account
            
        Returns:
            True if deletion was successful, False otherwise
        """
        # Refresh credentials if needed for target operations
        if is_target and self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        logs_client = self.target_logs_client if is_target else self.source_logs_client
        account_name = "target" if is_target else "source"
        
        try:
            logs_client.delete_log_group(logGroupName=log_group_name)
            logger.info(f"Successfully deleted log group in {account_name} account: {log_group_name}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting log group in {account_name} account: {e}")
            
            # Try refreshing credentials and retry if it's a credential error
            if self._handle_credential_error(e, is_target):
                # Retry with refreshed credentials
                logs_client = self.target_logs_client if is_target else self.source_logs_client
                try:
                    logs_client.delete_log_group(logGroupName=log_group_name)
                    logger.info(f"Successfully deleted log group in {account_name} account after credential refresh: {log_group_name}")
                    return True
                except ClientError as retry_e:
                    logger.error(f"Error deleting log group after credential refresh: {retry_e}")
            
            return False
            
    def update_tracker(
        self, 
        index: str, 
        checkpoint: Optional[str] = None, 
        status: Optional[str] = None, 
        error: Optional[str] = None, 
        src_doc: Optional[int] = None, 
        dest_doc: Optional[int] = None
    ) -> None:
        """
        Update the migration tracker file for a specific index.
        
        Args:
            index: Name of the index being processed
            checkpoint: Current checkpoint in the migration process
            status: Current status of the migration
            error: Error message if migration failed
            src_doc: Document count in source
            dest_doc: Document count in destination
        """
        with open(self.migration_tracker_file, 'r') as f:
            tracker_data = json.load(f)
        
        # Find the index entry or create a new one
        index_exists = False
        for i, entry in enumerate(tracker_data["migration_status"]):
            if entry.get("index") == index:
                # Update existing entry
                if checkpoint:
                    tracker_data["migration_status"][i]["checkpoint"] = checkpoint
                if status:
                    tracker_data["migration_status"][i]["status"] = status
                if error:
                    tracker_data["migration_status"][i]["error"] = error
                if src_doc is not None:
                    tracker_data["migration_status"][i]["src_doc"] = src_doc
                if dest_doc is not None:
                    tracker_data["migration_status"][i]["dest_doc"] = dest_doc
                
                tracker_data["migration_status"][i]["last_updated"] = datetime.now().isoformat()
                index_exists = True
                break
        
        if not index_exists:
            # Create new entry
            new_entry = {
                "index": index,
                "checkpoint": checkpoint or "INITIALIZING",
                "status": status or "ONGOING",
                "last_updated": datetime.now().isoformat(),
                "start_time": datetime.now().isoformat(),
            }
            if error:
                new_entry["error"] = error
            if src_doc is not None:
                new_entry["src_doc"] = src_doc
            if dest_doc is not None:
                new_entry["dest_doc"] = dest_doc
            
            tracker_data["migration_status"].append(new_entry)
        
        # Write updated tracker
        with open(self.migration_tracker_file, 'w') as f:
            json.dump(tracker_data, f, indent=4)
            
        logger.debug(f"Updated tracker for index {index}: checkpoint={checkpoint}, status={status}")