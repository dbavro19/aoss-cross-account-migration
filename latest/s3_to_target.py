import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple
from botocore.exceptions import ClientError

from base_migration import BaseMigration

# Configure logging
logger = logging.getLogger(__name__)

class S3ToTargetMigration(BaseMigration):
    """Class to handle S3 to OpenSearch target migration operations."""
    
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
        # Refresh credentials if needed for target operations
        if self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
            
        use_logging = self.create_log_group(log_group_name, is_target=True)
        time.sleep(10)

        # Use the target role for the target account operations
        target_role = self.target_role
        
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
                        sts_role_arn: {target_role}
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
                        sts_role_arn: {target_role}
                        
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
            logger.info(f"Creating pipeline in target account with role: {target_role}")
            response = self.target_pipeline_client.create_pipeline(**pipeline_args)
            logger.info(f"Created destination pipeline in target account: {pipeline_name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating pipeline in target account: {e}")
            logger.debug(f"Pipeline configuration used:\n{pipeline_body}")
            
            # Try refreshing credentials and retry if it's a credential error
            if self._handle_credential_error(e, True):
                # Retry with refreshed credentials
                try:
                    response = self.target_pipeline_client.create_pipeline(**pipeline_args)
                    logger.info(f"Created destination pipeline in target account after credential refresh: {pipeline_name}")
                    return response
                except ClientError as retry_e:
                    logger.error(f"Error creating pipeline after credential refresh: {retry_e}")
                    logger.debug(f"Pipeline configuration used:\n{pipeline_body}")
            
            raise
            
    async def migrate_from_s3_to_target(
        self, 
        index: str, 
        s3_prefix: str,
        source_host: str
    ) -> Tuple[str, datetime]:
        """
        Migrate data from S3 to target OpenSearch.
        
        Args:
            index: Name of the index to create in target
            s3_prefix: S3 prefix where source data is stored
            source_host: Source OpenSearch host for document count comparison
            
        Returns:
            Tuple containing destination pipeline name and migration start time
        """
        logger.info(f"Starting migration of index {index} from S3 to target OpenSearch")
        
        # Update checkpoint in tracker
        self.update_tracker(index, checkpoint="CREATING_DESTINATION_PIPELINE")
        
        # Refresh credentials before creating destination pipeline
        if self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
        
        # Create pipeline configuration
        pipeline_index = index.replace('_', '-')
        dest_pipeline_name = f"s3-oss-pipe-{pipeline_index[:6]}-{s3_prefix.split('-')[-1].rstrip('/')}"
        dest_log_group_name = self.target_pipeline_logs
        dest_metric_name = f'{dest_pipeline_name}.opensearch.recordsIn.count'
        
        # Create the pipeline
        loop = asyncio.get_event_loop()
        logger.info(f"Creating destination pipeline in target account: {dest_pipeline_name}")
        
        await loop.run_in_executor(
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
        
        # Wait for pipeline to be active
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
                source_host, 
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
        logger.info(f"Successfully migrated index {index} from S3 to target OpenSearch")
        
        return dest_pipeline_name, target_migration_start_time
        
    async def cleanup_target_pipeline(self, pipeline_name: str) -> None:
        """
        Clean up the target pipeline after migration.
        
        Args:
            pipeline_name: Name of the pipeline to delete
        """
        logger.info(f"Cleaning up target pipeline: {pipeline_name}")
        
        # Refresh credentials before deleting destination pipeline
        if self.source_account_id != self.target_account_id:
            self.refresh_target_credentials_if_needed()
        
        loop = asyncio.get_event_loop()
        
        # Delete the destination pipeline
        await loop.run_in_executor(
            None, 
            lambda: self.delete_pipeline(pipeline_name, is_target=True)
        )
        
        logger.info(f"Target pipeline {pipeline_name} deleted successfully")