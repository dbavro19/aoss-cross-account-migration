import asyncio
import logging
import random
import string
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple, List
from botocore.exceptions import ClientError

from base_migration import BaseMigration

# Configure logging
logger = logging.getLogger(__name__)

class SourceToS3Migration(BaseMigration):
    """Class to handle OpenSearch to S3 migration operations."""
    
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
            
    async def migrate_to_s3(self, index: str) -> Tuple[str, str, datetime]:
        """
        Migrate a single index from source OpenSearch to S3.
        
        Args:
            index: Name of the index to migrate
            
        Returns:
            Tuple containing S3 prefix, pipeline name, and migration start time
        """
        logger.info(f"Starting migration of index {index} to S3")
        
        # Update tracker
        self.update_tracker(
            index, 
            checkpoint="CREATING_SOURCE_PIPELINE", 
            status="ONGOING"
        )
        
        # Generate random characters for unique pipeline and S3 prefix names
        random_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        
        # Create pipeline configuration
        pipeline_index = index.replace('_', '-')
        s3_prefix = f"src-{pipeline_index[:6]}-{random_chars}/"
        src_pipeline_name = f"oss-s3-pipe-{pipeline_index[:6]}-{random_chars}"
        src_log_group_name = self.src_pipeline_logs
        source_metric_name = f'{src_pipeline_name}.s3.recordsIn.count'
        
        # Create the pipeline
        loop = asyncio.get_event_loop()
        logger.info(f"Creating source pipeline in source account: {src_pipeline_name}")
        
        await loop.run_in_executor(
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
        
        # Wait for pipeline to be active
        pipeline_active = await loop.run_in_executor(
            None,
            lambda: self.wait_for_pipeline_active(src_pipeline_name, is_target=False, max_attempts=40, delay_seconds=60)
        )
        
        migration_start_time = datetime.now(timezone.utc) - timedelta(minutes=1)
        
        if not pipeline_active:
            raise Exception(f"Source pipeline {src_pipeline_name} in source account failed to become active")

        self.update_tracker(index, checkpoint="CREATED_SOURCE_PIPELINE")
 
        # Monitor migration progress
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
        logger.info(f"Successfully migrated index {index} to S3 at prefix {s3_prefix}")
        
        return s3_prefix, src_pipeline_name, migration_start_time
        
    async def cleanup_source_pipeline(self, pipeline_name: str) -> None:
        """
        Clean up the source pipeline after migration.
        
        Args:
            pipeline_name: Name of the pipeline to delete
        """
        logger.info(f"Cleaning up source pipeline: {pipeline_name}")
        loop = asyncio.get_event_loop()
        
        # Delete the source pipeline
        await loop.run_in_executor(
            None, 
            lambda: self.delete_pipeline(pipeline_name, is_target=False)
        )
        
        logger.info(f"Source pipeline {pipeline_name} deleted successfully")