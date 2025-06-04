import asyncio
import boto3
import json
import logging
import os
import random
import requests
import string
import sys
import time
from datetime import datetime, timezone, timedelta
from requests_aws4auth import AWS4Auth
from typing import Dict, List, Tuple, Optional, Any, Union
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OpenSearchCrossAccountMigration:
    """Class to handle OpenSearch Serverless cross-account migration operations."""
    
    def __init__(self, config_file: str):
        """
        Initialize the OpenSearch cross-account migration tool.
        
        Args:
            config_file: Path to the JSON configuration file
        """
        # Load configuration from JSON file
        with open(config_file, 'r') as f:
            config = json.load(f)
        
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
        #self.target_vpc_config = config['target'].get('vpc_config', {})
        self.target_network_policy_name = config['target']['network_policy_name']
        
        # Shared configuration
        self.s3_bucket_name = config['shared']['s3_bucket_name']
        self.s3_bucket_region = config['shared']['s3_bucket_region']
        self.max_concurrent = config['shared'].get('max_concurrent', 3)
        self.migration_tracker_file = config['shared'].get('migration_tracker_file', "migration_tracker.json")
        
        # Set log level based on verbosity
        if config['shared'].get('verbose', False):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize source account AWS clients
        self.source_session = boto3.Session(region_name=self.source_region)
        self.source_credentials = self.source_session.get_credentials()
        
        self.source_pipeline_client = self.source_session.client('osis', region_name=self.source_region)
        self.source_logs_client = self.source_session.client('logs', region_name=self.source_region)
        self.source_cloudwatch_client = self.source_session.client('cloudwatch', region_name=self.source_region)
        
        # Initialize target account AWS clients using assumed role
        if self.source_account_id != self.target_account_id:
            self.target_session = self._create_target_session()
        if self.source_account_id == self.target_account_id:
            self.target_session = self.source_session
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
    
    def _create_target_session(self) -> boto3.Session:
        """
        Create a boto3 session for the target account by assuming a role.
        
        Returns:
            boto3.Session: Session for the target account
        """
        try:
            sts_client = self.source_session.client('sts')
            assumed_role = sts_client.assume_role(
                RoleArn=self.target_role,
                RoleSessionName="OpenSearchMigrationSession"
            )
            
            credentials = assumed_role['Credentials']
            
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
        url = f"{host}/_cat/indices/{index_name}?format=json"
        auth = self.target_awsauth if is_target else self.source_awsauth
        
        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            return int(response.json()[0]['docs.count'])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting index document count: {e}")
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
                return False
                
    def create_src_ingestion_pipeline(
        self, 
        pipeline_name: str, 
        source_opensearch_endpoint: str, 
        index_name: str, 
        s3_prefix: str, 
        iam_role: str, 
        log_group_name: str
    ) -> Dict[str, Any]:
        """
        Create a pipeline in the source account to ingest data from OpenSearch to S3.
        
        Args:
            pipeline_name: Name of the pipeline to create
            source_opensearch_endpoint: Source OpenSearch endpoint URL
            index_name: Name of the index to migrate
            s3_prefix: S3 prefix for storing data
            iam_role: IAM role ARN for pipeline execution
            log_group_name: CloudWatch log group name
            
        Returns:
            Response from the create_pipeline API call
        """
        use_logging = self.create_log_group(log_group_name, is_target=False)
        time.sleep(10)
        
        # Base pipeline configuration
        pipeline_body = f"""
        version: '2'
        {pipeline_name}:
            source:
                opensearch:
                    acknowledgments: true
                    hosts:
                        - {source_opensearch_endpoint}
                    aws:
                        serverless: true
                        region: {self.source_region}
                        sts_role_arn: {iam_role}
                        serverless_options:
                            network_policy_name: {self.source_network_policy_name}
                    indices:
                        include:
                            - index_name_regex: {index_name}
            sink:
                - s3:
                    bucket: {self.s3_bucket_name}
                    bucket_owners:
                        {self.s3_bucket_name}: {self.target_account_id}
                    object_key:
                        path_prefix: {s3_prefix}
                    compression: none
                    codec:
                        json: {{}}
                    threshold:
                        event_collect_timeout: 300s
                        maximum_size: 100mb
                    aws:
                        region: {self.s3_bucket_region}
                        sts_role_arn: {iam_role}
        """
        
        pipeline_args = {
            'PipelineName': pipeline_name,
            'MinUnits': 1,
            'MaxUnits': 5,
            'PipelineConfigurationBody': pipeline_body
        }
        
        # Add VPC configuration if provided
        if self.source_vpc_config and self.source_vpc_config.get('vpc_id') and self.source_vpc_config.get('subnet_ids') and self.source_vpc_config.get('security_group_ids'):
            pipeline_args['VpcOptions'] = {
                'SubnetIds': self.source_vpc_config['subnet_ids'],
                'SecurityGroupIds': self.source_vpc_config['security_group_ids']
            }
            logger.info(f"Adding VPC configuration to source pipeline: {self.source_vpc_config['vpc_id']}")
        
            if self.source_vpc_config and self.source_vpc_config.get('vpc_attachment_options'):
                pipeline_args['VpcOptions']['VpcAttachmentOptions'] = self.source_vpc_config.get('vpc_attachment_options')
                pipeline_args['VpcOptions']['VpcEndpointManagement'] = self.source_vpc_config.get('vpc_endpoint_management')
                 
        if use_logging:
            pipeline_args['LogPublishingOptions'] = {
                'IsLoggingEnabled': True,
                'CloudWatchLogDestination': {
                    'LogGroup': log_group_name
                }
            }
        else:
            pipeline_args['LogPublishingOptions'] = {
                'IsLoggingEnabled': False
            }

        try:
            response = self.source_pipeline_client.create_pipeline(**pipeline_args)
            logger.info(f"Created source pipeline in source account: {pipeline_name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating pipeline in source account: {e}")
            logger.debug(f"Pipeline configuration used:\n{pipeline_body}")
            raise

    def create_dest_ingestion_pipeline(
        self,
        pipeline_name: str, 
        opensearch_endpoint: str, 
        s3_prefix: str, 
        index_name: str, 
        iam_role: str, 
        log_group_name: str
    ) -> Dict[str, Any]:
        """
        Create a pipeline in the target account to ingest data from S3 to OpenSearch.
        
        Args:
            pipeline_name: Name of the pipeline to create
            opensearch_endpoint: Destination OpenSearch endpoint URL
            s3_prefix: S3 prefix for retrieving data
            index_name: Name of the index to create
            iam_role: IAM role ARN for pipeline execution
            log_group_name: CloudWatch log group name
            
        Returns:
            Response from the create_pipeline API call
        """
        use_logging = self.create_log_group(log_group_name, is_target=True)
        time.sleep(10)

        pipeline_body = f"""
        version: '2'
        {pipeline_name}:
            source:
                s3:
                    acknowledgments: true
                    delete_s3_objects_on_read: false
                    scan:
                        buckets:
                            - bucket:
                                name: {self.s3_bucket_name}
                                filter:
                                    include_prefix: 
                                        - {s3_prefix}
                    aws:
                        region: {self.s3_bucket_region}
                        sts_role_arn: {iam_role}
                    codec:
                        json: {{}}
                    compression: none
                    workers: 1
            sink:
                - opensearch:
                    hosts:
                        - {opensearch_endpoint}
                    aws:
                        serverless: true
                        region: {self.target_region}
                        sts_role_arn: {iam_role}
                        
                        serverless_options:
                            network_policy_name: {self.target_network_policy_name}
                    index_type: custom
                    index: {index_name}
        """
        
        pipeline_args = {
            'PipelineName': pipeline_name,
            'MinUnits': 1,
            'MaxUnits': 5,
            'PipelineConfigurationBody': pipeline_body
        }
        
        if use_logging:
            pipeline_args['LogPublishingOptions'] = {
                'IsLoggingEnabled': True,
                'CloudWatchLogDestination': {
                    'LogGroup': log_group_name
                }
            }
        else:
            pipeline_args['LogPublishingOptions'] = {
                'IsLoggingEnabled': False
            }

        try:
            response = self.target_pipeline_client.create_pipeline(**pipeline_args)
            logger.info(f"Created destination pipeline in target account: {pipeline_name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating pipeline in target account: {e}")
            logger.debug(f"Pipeline configuration used:\n{pipeline_body}")
            raise
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
        pipeline_client = self.target_pipeline_client if is_target else self.source_pipeline_client
        account_name = "target" if is_target else "source"
        
        logger.info(f"Waiting for pipeline '{pipeline_name}' in {account_name} account to become active...")
        
        for attempt in range(1, max_attempts + 1):
            try:
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
        pipeline_client = self.target_pipeline_client if is_target else self.source_pipeline_client
        account_name = "target" if is_target else "source"
        
        try:
            response = pipeline_client.delete_pipeline(PipelineName=pipeline_name)
            logger.info(f"Successfully deleted pipeline in {account_name} account: {pipeline_name}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting pipeline in {account_name} account: {e}")
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
        logs_client = self.target_logs_client if is_target else self.source_logs_client
        account_name = "target" if is_target else "source"
        
        try:
            logs_client.delete_log_group(logGroupName=log_group_name)
            logger.info(f"Successfully deleted log group in {account_name} account: {log_group_name}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting log group in {account_name} account: {e}")
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
            
    async def migrate_index(
        self, 
        index: str
    ) -> Tuple[List[str], List[str]]:
        """
        Asynchronously migrate a single index from source to destination OpenSearch Serverless.
        
        Args:
            index: Name of the index to migrate
            
        Returns:
            Tuple containing lists of created pipeline names and log group names
        """
        created_pipelines = []
        created_log_groups = []
        
        logger.info(f"Processing index: {index}")
        
        # 1. Create a json structure for tracking this index
        self.update_tracker(
            index, 
            checkpoint="CREATING_SOURCE_PIPELINE", 
            status="ONGOING"
        )
        
        try:
            loop = asyncio.get_event_loop()
            random_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
            
            # 2. Create a pipeline to move data from source opensearch serverless to S3
            pipeline_index = index.replace('_', '-')
            s3_prefix = f"src-{pipeline_index[:6]}-{random_chars}/"
            src_pipeline_name = f"oss-s3-pipe-{pipeline_index[:6]}-{random_chars}"
            src_log_group_name = f'/aws/vendedlogs/ossmigration/{src_pipeline_name}'
            source_metric_name = f'{src_pipeline_name}.s3.recordsIn.count'
            
            created_pipelines.append((src_pipeline_name, False))  # (name, is_target)
            created_log_groups.append((src_log_group_name, False))
            
            logger.info(f"Creating source pipeline in source account: {src_pipeline_name}")
            # Use asyncio.to_thread for blocking operations
            src_response = await loop.run_in_executor(
                None, 
                lambda: self.create_src_ingestion_pipeline(
                    pipeline_name=src_pipeline_name, 
                    source_opensearch_endpoint=self.source_host, 
                    index_name=index,
                    s3_prefix=s3_prefix, 
                    iam_role=self.source_role,
                    log_group_name=src_log_group_name, 
                )
            )
            
            # Update checkpoint in tracker
            self.update_tracker(index, checkpoint="CREATING_SOURCE_PIPELINE")
            
            # 3. Wait for pipeline to be active
            pipeline_active = await loop.run_in_executor(
                None,
                lambda: self.wait_for_pipeline_active(src_pipeline_name, is_target=False, max_attempts=40, delay_seconds=60)
            )
            
            migration_start_time = datetime.now(timezone.utc) - timedelta(minutes=1)
            
            if not pipeline_active:
                raise Exception(f"Source pipeline {src_pipeline_name} in source account failed to become active")

            self.update_tracker(index, checkpoint="CREATED_SOURCE_PIPELINE")
 
            migration_status = await loop.run_in_executor(
                None,
                lambda: self.get_migration_status(
                    self.source_host, 
                    index, 
                    src_pipeline_name, 
                    source_metric_name, 
                    migration_start_time, 
                    is_target=False,
                    period=3600,
                    delay_seconds=60,
                    timeout_minutes=120
                )
            )

            if not migration_status:
                self.update_tracker(index, checkpoint="FAILED MIGRATION TO S3")
                raise Exception(f"Source pipeline {src_pipeline_name} in source account - Migration Failed")

            self.update_tracker(index, checkpoint="MIGRATED TO S3")

            # S3 to OSS Migration
            # 4. Create a pipeline to move data from S3 to destination opensearch serverless
            dest_pipeline_name = f"s3-oss-pipe-{pipeline_index[:6]}-{random_chars}"
            dest_log_group_name = f'/aws/vendedlogs/ossmigration/{dest_pipeline_name}'
            dest_metric_name = f'{dest_pipeline_name}.opensearch.recordsIn.count'
            
            created_pipelines.append((dest_pipeline_name, True))  # (name, is_target)
            created_log_groups.append((dest_log_group_name, True))
            
            # Update checkpoint in tracker
            self.update_tracker(index, checkpoint="CREATING_DESTINATION_PIPELINE")
            
            logger.info(f"Creating destination pipeline in target account: {dest_pipeline_name}")
            dest_response = await loop.run_in_executor(
                None,
                lambda: self.create_dest_ingestion_pipeline(
                    pipeline_name=dest_pipeline_name,
                    opensearch_endpoint=self.target_host,
                    s3_prefix=s3_prefix,
                    index_name=index,
                    iam_role=self.target_role,
                    log_group_name=dest_log_group_name, 
                )
            )
            
            # 5. Wait for pipeline to be active
            pipeline_active = await loop.run_in_executor(
                None,
                lambda: self.wait_for_pipeline_active(dest_pipeline_name, is_target=True, max_attempts=40, delay_seconds=60)
            )

            target_migration_start_time = datetime.now(timezone.utc) - timedelta(minutes=1)
            
            if not pipeline_active:
                raise Exception(f"Destination pipeline {dest_pipeline_name} in target account failed to become active")
            
            # Get pipeline Migration Status
            oss_migration_status = await loop.run_in_executor(
                None,
                lambda: self.get_migration_status(
                    self.source_host, 
                    index, 
                    dest_pipeline_name, 
                    dest_metric_name, 
                    target_migration_start_time, 
                    is_target=True,
                    period=3600,
                    delay_seconds=60,
                    timeout_minutes=120
                )
            )

            if not oss_migration_status:
                self.update_tracker(index, checkpoint="FAILED MIGRATION TO TARGET OSS")
                raise Exception(f"Destination pipeline {dest_pipeline_name} in target account - Migration Failed")

            self.update_tracker(index, checkpoint="MIGRATED TO TARGET OSS")

            # Wait for data to settle
            await asyncio.sleep(120)

            # 6. Gather document count from source and destination
            src_doc_count = await loop.run_in_executor(
                None,
                lambda: self.get_index_doc_count(self.source_host, index)
            )
            logger.info(f"Source document count for {index}: {src_doc_count}")


            # Delete the source pipeline
            logger.info(f"Deleting source pipeline in source account: {src_pipeline_name}")
            await loop.run_in_executor(None, lambda: self.delete_pipeline(src_pipeline_name, is_target=False))
            logger.info(f"Deleting source pipeline log group in source account: {src_log_group_name}")
            await loop.run_in_executor(None, lambda: self.delete_log_group(src_log_group_name, is_target=False))

            # Update checkpoint in tracker
            self.update_tracker(index, checkpoint="DELETED_SOURCE_PIPELINE")

            # Delete the destination pipeline
            logger.info(f"Deleting destination pipeline in target account: {dest_pipeline_name}")
            await loop.run_in_executor(None, lambda: self.delete_pipeline(dest_pipeline_name, is_target=True))
            logger.info(f"Deleting destination pipeline log group in target account: {dest_log_group_name}")
            await loop.run_in_executor(None, lambda: self.delete_log_group(dest_log_group_name, is_target=True))

            # Update checkpoint in tracker
            self.update_tracker(index, checkpoint="DELETED_DESTINATION_PIPELINE")
            
            # Update status to DONE
            self.update_tracker(index, status="DONE", src_doc=src_doc_count)
            
            logger.info(f"Migration completed successfully for index {index}")
        
        except Exception as e:
            logger.error(f"Error processing index {index}: {e}")
            
            # Update status to FAILED
            self.update_tracker(index, status="FAILED", error=str(e))
        
        return created_pipelines, created_log_groups

    async def migrate_indices(self) -> Tuple[List[Tuple[str, bool]], List[Tuple[str, bool]]]:
        """
        Asynchronously migrate indices from source OpenSearch Serverless to destination.
        
        Returns:
            Tuple containing lists of created pipelines and log groups with their account flags
        """
        # Get Indices in the src opensearch Serverless
        logger.info('Getting indices from source OpenSearch Serverless...')
        indices = self.get_indices(self.source_host)
        
        if not indices:
            logger.warning("No indices found to migrate")
            return [], []
            
        logger.info(f'Found {len(indices)} indices to migrate')
        logger.info(f'Processing with max_concurrent={self.max_concurrent}')
        
        # Track resources for cleanup
        all_created_pipelines = []  # List of (pipeline_name, is_target) tuples
        all_created_log_groups = []  # List of (log_group_name, is_target) tuples
        
        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def limited_migrate_index(index):
            """Wrapper to limit concurrent executions with semaphore"""
            async with semaphore:
                logger.info(f"Starting migration for index: {index}")
                return await self.migrate_index(index)
        
        # Create tasks for all indices at once, but limit concurrent execution with semaphore
        tasks = [limited_migrate_index(index) for index in indices]
        
        # Wait for all tasks to complete
        logger.info(f"Starting migration of {len(tasks)} indices with max {self.max_concurrent} concurrent tasks...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect resources for cleanup and handle exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Migration for index {indices[i]} failed with exception: {result}")
            else:
                pipelines, log_groups = result
                all_created_pipelines.extend(pipelines)
                all_created_log_groups.extend(log_groups)
        return all_created_pipelines, all_created_log_groups
        
    def cleanup_resources(
        self, 
        pipeline_names: List[Tuple[str, bool]], 
        log_groups: List[Tuple[str, bool]], 
        delete_pipelines: bool = True
    ) -> None:
        """
        Clean up all resources created during migration in both accounts.
        
        Args:
            pipeline_names: List of (pipeline_name, is_target) tuples to delete
            log_groups: List of (log_group_name, is_target) tuples to delete
            delete_pipelines: Whether to delete pipelines or not
        """
        logger.info("Starting cleanup process...")
        
        # Delete pipelines
        if delete_pipelines:
            for pipeline_name, is_target in pipeline_names:
                try:
                    self.delete_pipeline(pipeline_name, is_target)
                except Exception as e:
                    account = "target" if is_target else "source"
                    logger.error(f"Error deleting pipeline {pipeline_name} in {account} account: {e}")
        
        # Delete log groups
        for log_group, is_target in log_groups:
            try:
                self.delete_log_group(log_group, is_target)
            except Exception as e:
                account = "target" if is_target else "source"
                logger.error(f"Error deleting log group {log_group} in {account} account: {e}")


async def main_async(config_file: str):
    """
    Main async function to run the migration.
    
    Args:
        config_file: Path to the JSON configuration file
    """
    try:
        migration = OpenSearchCrossAccountMigration(config_file)
        
        logger.info("Starting OpenSearch Serverless cross-account migration...")
        created_pipelines, created_log_groups = await migration.migrate_indices()
        print(created_pipelines)
        print(created_log_groups)
        
        logger.info("Migration process completed.")
        
        # Ask user if they want to clean up resources
        cleanup_choice = input("Do you want to clean up any remaining resources? (y/n): ")
        if cleanup_choice.lower() == 'y':
            migration.cleanup_resources(created_pipelines, created_log_groups)
            logger.info("Cleanup completed.")
        else:
            logger.info("Skipping cleanup. Resources will remain active.")
            
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        sys.exit(1)


def main():
    """Entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='OpenSearch Serverless Cross-Account Migration Tool')
    parser.add_argument('--config', '-c', required=True, help='Path to the JSON configuration file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        asyncio.run(main_async(args.config))
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user.")
        sys.exit(1)


if __name__ == "__main__":
    main()
