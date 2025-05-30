#!/usr/bin/env python3
"""
OpenSearch Serverless Migration Tool

This script migrates indices from a source OpenSearch Serverless collection to a target
OpenSearch Serverless collection using AWS OpenSearch Ingestion pipelines and S3 as an
intermediate storage.
"""

import argparse
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OpenSearchMigration:
    """Class to handle OpenSearch Serverless migration operations."""
    
    def __init__(self, config: Dict[str, str], max_concurrent: int = 3):
        """
        Initialize the OpenSearch migration tool.
        
        Args:
            config: Dictionary containing configuration parameters
            max_concurrent: Maximum number of indices to migrate concurrently
        """
        self.source_host = config['source_host']
        self.source_ingestion_role = config['source_ingestion_role']
        self.target_host = config['target_host']
        self.target_ingestion_role = config['target_ingestion_role']
        self.source_region = config['source_region']
        self.target_region = config['target_region']
        self.s3_bucket_name = config['s3_bucket_name']
        self.max_concurrent = max_concurrent
        
        # Initialize AWS clients
        self.credentials = boto3.Session().get_credentials()
        self.pipeline_client = boto3.client('osis', region_name=self.source_region)
        self.logs_client = boto3.client('logs', region_name=self.source_region)
        self.s3_client = boto3.client('s3')
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.source_region)
        
        # Initialize AWS4Auth for OpenSearch API requests
        self.awsauth = AWS4Auth(
            self.credentials.access_key,
            self.credentials.secret_key,
            self.source_region,
            'aoss',
            session_token=self.credentials.token
        )
        
        # Initialize migration tracker
        self.migration_tracker_file = config.get('migration_tracker_file', "migration_tracker.json")
        self._init_migration_tracker()
    
    def _init_migration_tracker(self) -> None:
        """Initialize the migration tracker file if it doesn't exist."""
        if not os.path.exists(self.migration_tracker_file):
            with open(self.migration_tracker_file, 'w') as f:
                json.dump({
                    "migration_status": [],
                    "summary": {
                        "total": 0,
                        "passed": 0,
                        "failed": 0,
                        "ongoing": 0,
                        "start_time": datetime.now().isoformat(),
                        "last_updated": datetime.now().isoformat()
                    }
                }, f, indent=4)
            logger.info(f"Created migration tracker file: {self.migration_tracker_file}")
        else:
            # Make sure the summary section exists
            with open(self.migration_tracker_file, 'r') as f:
                tracker_data = json.load(f)
            
            if "summary" not in tracker_data:
                tracker_data["summary"] = {
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                    "ongoing": 0,
                    "start_time": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat()
                }
                
                with open(self.migration_tracker_file, 'w') as f:
                    json.dump(tracker_data, f, indent=4)

    def get_indices(self, host: str) -> List[str]:
        """
        Get list of indices from an OpenSearch Serverless endpoint.
        
        Args:
            host: OpenSearch Serverless endpoint URL
            
        Returns:
            List of index names
        """
        url = f"{host}/_cat/indices?format=json"
        response = requests.get(url, auth=self.awsauth)
        
        if response.status_code != 200:
            logger.error(f"Error getting indices: {response.text}")
            return []
            
        indices = [doc['index'] for doc in response.json()]
        logger.info(f"Found {len(indices)} indices")
        return indices

    def get_index_doc_count(self, host: str, index_name: str) -> int:
        """
        Get document count for a specific index.
        
        Args:
            host: OpenSearch Serverless endpoint URL
            index_name: Name of the index
            
        Returns:
            Document count as integer
        """
        url = f"{host}/_cat/indices/{index_name}?format=json"
        response = requests.get(url, auth=self.awsauth)
        
        if response.status_code != 200:
            logger.error(f"Error getting index document count: {response.text}")
            return 0
            
        return int(response.json()[0]['docs.count'])

    def create_log_group(self, log_group_name: str) -> bool:
        """
        Create a CloudWatch log group.
        
        Args:
            log_group_name: Name of the log group to create
            
        Returns:
            True if log group was created or already exists, False otherwise
        """
        try:
            self.logs_client.create_log_group(logGroupName=log_group_name)
            logger.info(f"Created log group: {log_group_name}")
            return True
        except self.logs_client.exceptions.ResourceAlreadyExistsException:
            logger.info(f"Log group already exists: {log_group_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating log group: {e}")
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
        Create a pipeline to ingest data from OpenSearch to S3.
        
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
        use_logging = self.create_log_group(log_group_name)
        time.sleep(10)
            
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
                            network_policy_name: ''
                    indices:
                        include:
                            - index_name_regex: {index_name}
            sink:
                - s3:
                    bucket: {self.s3_bucket_name}
                    object_key:
                        path_prefix: {s3_prefix}
                    compression: none
                    codec:
                        json: {{}}
                    threshold:
                        event_collect_timeout: 300s
                        maximum_size: 100mb
                    aws:
                        region: {self.target_region}
                        sts_role_arn: {iam_role}
        """
        pipeline_args = {
            'PipelineName': pipeline_name,
            'MinUnits': 1,
            'MaxUnits': 50,
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
            response = self.pipeline_client.create_pipeline(**pipeline_args)
            logger.info(f"Created source pipeline: {pipeline_name}")
            return response
        except Exception as e:
            logger.error(f"Error creating pipeline: {e}")
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
        Create a pipeline to ingest data from S3 to OpenSearch.
        
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
        use_logging = self.create_log_group(log_group_name)
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
                        region: {self.target_region}
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
                            network_policy_name: ''
                    index_type: custom
                    index: {index_name}
        """
        pipeline_args = {
            'PipelineName': pipeline_name,
            'MinUnits': 1,
            'MaxUnits': 50,
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
            response = self.pipeline_client.create_pipeline(**pipeline_args)
            logger.info(f"Created destination pipeline: {pipeline_name}")
            return response
        except Exception as e:
            logger.error(f"Error creating pipeline: {e}")
            logger.debug(f"Pipeline configuration used:\n{pipeline_body}")
            raise
            
    def wait_for_pipeline_active(
        self, 
        pipeline_name: str, 
        max_attempts: int = 30, 
        delay_seconds: int = 10
    ) -> bool:
        """
        Poll the pipeline status until it becomes ACTIVE or reaches max attempts.
        
        Args:
            pipeline_name: Name of the pipeline to monitor
            max_attempts: Maximum number of polling attempts
            delay_seconds: Delay between polling attempts in seconds
            
        Returns:
            True if pipeline is active, False otherwise
        """
        logger.info(f"Waiting for pipeline '{pipeline_name}' to become active...")
        
        for attempt in range(1, max_attempts + 1):
            try:
                response = self.pipeline_client.get_pipeline(PipelineName=pipeline_name)
                status = response['Pipeline'].get('Status')
                
                logger.info(f"Attempt {attempt}/{max_attempts}: Pipeline: {pipeline_name} status: {status}")
                
                if status == 'ACTIVE':
                    logger.info(f"Pipeline '{pipeline_name}' is now ACTIVE!")
                    return True
                elif status in ['FAILED', 'DELETED']:
                    logger.error(f"Pipeline reached terminal state: {status}")
                    return False
                    
                # Wait before next check
                time.sleep(delay_seconds)
                
            except Exception as e:
                logger.error(f"Error checking pipeline status: {e}")
                time.sleep(delay_seconds)
        
        logger.error(f"Timed out waiting for pipeline '{pipeline_name}' to become active")
        return False
        
    def get_pipeline_metric_stats(
        self, 
        pipeline_name: str, 
        metric_name: str, 
        start_time: datetime, 
        end_time: datetime, 
        period: int = 60
    ) -> int:
        """
        Get metric statistics from CloudWatch for an OpenSearch Ingestion Pipeline.

        Args:
            pipeline_name: Name of the OpenSearch Ingestion Pipeline
            metric_name: Name of the metric to query (e.g., 'recordsIn.count')
            start_time: Start time for the metric query
            end_time: End time for the metric query
            period: Period in seconds for the metric aggregation

        Returns:
            Total sum of the metric values
        """
        logger.info(f'Getting metrics for {pipeline_name} - {metric_name}')
        logger.info(f'Time range: {start_time} to {end_time}')
        
        response = self.cloudwatch_client.get_metric_statistics(
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
    def get_migration_status(
        self,
        host: str,
        index_name: str, 
        pipeline_name: str, 
        metric_name: str, 
        start_time: datetime,
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
        
        while True:
            check_count += 1
            # Check if we've exceeded the timeout
            if datetime.now() - start_check_time > timeout_delta:
                logger.error(f"Migration timed out after {timeout_minutes} minutes")
                return False
                
            # Update end_time to current time for each check
            current_end_time = datetime.now(timezone.utc)
            
            # Get current metrics count
            processed_count = self.get_pipeline_metric_stats(
                pipeline_name, 
                metric_name, 
                start_time, 
                current_end_time, 
                period
            )
            
            # Check if migration is complete
            if processed_count >= src_docs_count:
                logger.info(f"Source documents and processed records match - {index_name}: {src_docs_count}")
                return True
                
            # Log progress
            logger.info(f"Check #{check_count}: Source document count: {src_docs_count}, "
                       f"Processed records: {processed_count} - index: {index_name}")
            logger.info(f"Migration progress: {processed_count}/{src_docs_count} "
                       f"({(processed_count/src_docs_count*100):.2f}%) - {index_name}")
            
            # Wait before next check
            time.sleep(delay_seconds)

    def delete_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Delete an OpenSearch Ingestion Pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to delete
            
        Returns:
            Response from the delete_pipeline API call
        """
        try:
            response = self.pipeline_client.delete_pipeline(PipelineName=pipeline_name)
            logger.info(f"Successfully deleted pipeline: {pipeline_name}")
            return response
        except Exception as e:
            logger.error(f"Error deleting pipeline: {e}")
            raise

    def delete_log_group(self, log_group_name: str) -> bool:
        """
        Delete a CloudWatch log group.

        Args:
            log_group_name: Name of the log group to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            self.logs_client.delete_log_group(logGroupName=log_group_name)
            logger.info(f"Successfully deleted log group: {log_group_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting log group {log_group_name}: {e}")
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
        old_status = None
        
        for i, entry in enumerate(tracker_data["migration_status"]):
            if entry.get("index") == index:
                # Save old status for summary update
                old_status = entry.get("status")
                
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
                "result": "PENDING"
            }
            if error:
                new_entry["error"] = error
            if src_doc is not None:
                new_entry["src_doc"] = src_doc
            if dest_doc is not None:
                new_entry["dest_doc"] = dest_doc
            
            tracker_data["migration_status"].append(new_entry)
            
            # Update summary for new entry
            tracker_data["summary"]["total"] += 1
            tracker_data["summary"]["ongoing"] += 1
        
        # Update summary based on status changes
        if status and old_status != status:
            # If status changed to DONE, increment passed and decrement ongoing
            if status == "DONE":
                tracker_data["summary"]["passed"] += 1
                tracker_data["summary"]["ongoing"] -= 1
                
                # Update result field for the index
                for i, entry in enumerate(tracker_data["migration_status"]):
                    if entry.get("index") == index:
                        tracker_data["migration_status"][i]["result"] = "PASS"
                
            # If status changed to FAILED, increment failed and decrement ongoing
            elif status == "FAILED":
                tracker_data["summary"]["failed"] += 1
                tracker_data["summary"]["ongoing"] -= 1
                
                # Update result field for the index
                for i, entry in enumerate(tracker_data["migration_status"]):
                    if entry.get("index") == index:
                        tracker_data["migration_status"][i]["result"] = "FAIL"
        
        # Update last_updated timestamp in summary
        tracker_data["summary"]["last_updated"] = datetime.now().isoformat()
        
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
            src_log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{src_pipeline_name}'
            source_metric_name = f'{src_pipeline_name}.s3.recordsIn.count'
            
            created_pipelines.append(src_pipeline_name)
            created_log_groups.append(src_log_group_name)
            
            logger.info(f"Creating source pipeline: {src_pipeline_name}")
            # Use asyncio.to_thread for blocking operations
            src_response = await loop.run_in_executor(
                None, 
                lambda: self.create_src_ingestion_pipeline(
                    pipeline_name=src_pipeline_name, 
                    source_opensearch_endpoint=self.source_host, 
                    index_name=index,
                    s3_prefix=s3_prefix, 
                    iam_role=self.source_ingestion_role,
                    log_group_name=src_log_group_name, 
                )
            )
            
            # Update checkpoint in tracker
            self.update_tracker(index, checkpoint="CREATING_SOURCE_PIPELINE")
            
            # 3. Wait for pipeline to be active
            pipeline_active = await loop.run_in_executor(
                None,
                lambda: self.wait_for_pipeline_active(src_pipeline_name, max_attempts=40, delay_seconds=60)
            )
            
            migration_start_time = datetime.now(timezone.utc) - timedelta(minutes=1)
            
            if not pipeline_active:
                raise Exception(f"Source pipeline {src_pipeline_name} failed to become active")

            self.update_tracker(index, checkpoint="CREATED_SOURCE_PIPELINE")
 
            migration_status = await loop.run_in_executor(
                None,
                lambda: self.get_migration_status(
                    self.source_host, 
                    index, 
                    src_pipeline_name, 
                    source_metric_name, 
                    migration_start_time, 
                    period=3600,
                    delay_seconds=300,
                    timeout_minutes=10080
                )
            )

            if not migration_status:
                self.update_tracker(index, checkpoint="FAILED MIGRATION TO S3")
                raise Exception(f"Source pipeline {src_pipeline_name} - Migration Failed")

            self.update_tracker(index, checkpoint="MIGRATED TO S3")

            # S3 to OSS Migration
            # 4. Create a pipeline to move data from S3 to destination opensearch serverless
            dest_pipeline_name = f"s3-oss-pipe-{pipeline_index[:6]}-{random_chars}"
            dest_log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{dest_pipeline_name}'
            dest_metric_name = f'{dest_pipeline_name}.opensearch.recordsIn.count'
            
            created_pipelines.append(dest_pipeline_name)
            created_log_groups.append(dest_log_group_name)
            
            # Update checkpoint in tracker
            self.update_tracker(index, checkpoint="CREATING_DESTINATION_PIPELINE")
            
            logger.info(f"Creating destination pipeline: {dest_pipeline_name}")
            dest_response = await loop.run_in_executor(
                None,
                lambda: self.create_dest_ingestion_pipeline(
                    pipeline_name=dest_pipeline_name,
                    opensearch_endpoint=self.target_host,
                    s3_prefix=s3_prefix,
                    index_name=index,
                    iam_role=self.target_ingestion_role,
                    log_group_name=dest_log_group_name, 
                )
            )
            
            # 5. Wait for pipeline to be active
            pipeline_active = await loop.run_in_executor(
                None,
                lambda: self.wait_for_pipeline_active(dest_pipeline_name, max_attempts=40, delay_seconds=60)
            )

            target_migration_start_time = datetime.now(timezone.utc) - timedelta(minutes=1)
            
            if not pipeline_active:
                raise Exception(f"Destination pipeline {dest_pipeline_name} failed to become active")
            
            # Get pipeline Migration Status
            oss_migration_status = await loop.run_in_executor(
                None,
                lambda: self.get_migration_status(
                    self.source_host, 
                    index, 
                    dest_pipeline_name, 
                    dest_metric_name, 
                    target_migration_start_time, 
                    period=3600,
                    delay_seconds=300,
                    timeout_minutes=10080
                )
            )

            if not oss_migration_status:
                self.update_tracker(index, checkpoint="FAILED MIGRATION TO TARGET OSS")
                raise Exception(f"Destination pipeline {dest_pipeline_name} - Migration Failed")

            self.update_tracker(index, checkpoint="MIGRATED TO TARGET OSS")

            # Wait for data to settle
            await asyncio.sleep(60)

            # 6. Gather document count from source and destination
            src_doc_count = await loop.run_in_executor(
                None,
                lambda: self.get_index_doc_count(self.source_host, index)
            )
            
            dest_doc_count = await loop.run_in_executor(
                None,
                lambda: self.get_index_doc_count(self.target_host, index)
            )
            
            logger.info(f"Source document count for {index}: {src_doc_count}")
            logger.info(f"Destination document count for {index}: {dest_doc_count}")
            
            # 7. If document count is same
            if src_doc_count == dest_doc_count and src_doc_count > 0:
                logger.info(f"Document counts match for index: {index}")

                # Delete the source pipeline
                logger.info(f"Deleting source pipeline: {src_pipeline_name}")
                await loop.run_in_executor(None, lambda: self.delete_pipeline(src_pipeline_name))
                logger.info(f"Deleting source pipeline log group: {src_log_group_name}")
                await loop.run_in_executor(None, lambda: self.delete_log_group(src_log_group_name))

                # Update checkpoint in tracker
                self.update_tracker(index, checkpoint="DELETED_SOURCE_PIPELINE")

                # Delete the destination pipeline
                logger.info(f"Deleting destination pipeline: {dest_pipeline_name}")
                await loop.run_in_executor(None, lambda: self.delete_pipeline(dest_pipeline_name))
                logger.info(f"Deleting destination pipeline log group: {dest_log_group_name}")
                await loop.run_in_executor(None, lambda: self.delete_log_group(dest_log_group_name))

                # Update checkpoint in tracker
                self.update_tracker(index, checkpoint="DELETED_DESTINATION_PIPELINE")
                
                # Update status to DONE
                self.update_tracker(index, status="DONE", src_doc=src_doc_count, dest_doc=dest_doc_count)
                
                logger.info(f"Migration completed successfully for index {index}")
                
            # 8. If document count is not equal
            else:
                # Update status to FAILED
                self.update_tracker(
                    index, 
                    status="FAILED", 
                    src_doc=src_doc_count, 
                    dest_doc=dest_doc_count
                )
                
                logger.error(f"Migration failed for index {index}. Document counts don't match.")
                logger.error(f"Source documents: {src_doc_count}")
                logger.error(f"Destination documents: {dest_doc_count}")
        
        except Exception as e:
            logger.error(f"Error processing index {index}: {e}")
            
            # Update status to FAILED
            self.update_tracker(index, status="FAILED", error=str(e))
        
        return created_pipelines, created_log_groups
    
    async def migrate_indices(self) -> Tuple[List[str], List[str]]:
        """
        Asynchronously migrate indices from source OpenSearch Serverless to destination.
        
        Returns:
            Tuple containing lists of created pipelines and log groups
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
        all_created_pipelines = []
        all_created_log_groups = []
        
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
        
        # Generate final summary
        self.generate_migration_summary()
        
        return all_created_pipelines, all_created_log_groups
        
    def cleanup_resources(self, pipeline_names: List[str], log_groups: List[str], delete_pipelines: bool = True) -> None:
        """
        Clean up all resources created during migration.
        
        Args:
            pipeline_names: List of pipeline names to delete
            log_groups: List of CloudWatch log groups to delete
            delete_pipelines: Whether to delete pipelines or not
        """
        logger.info("Starting cleanup process...")
        
        # Delete pipelines
        if delete_pipelines:
            for pipeline_name in pipeline_names:
                try:
                    self.delete_pipeline(pipeline_name)
                except Exception as e:
                    logger.error(f"Error deleting pipeline {pipeline_name}: {e}")
        
        # Delete log groups
        for log_group in log_groups:
            try:
                self.delete_log_group(log_group)
            except Exception as e:
                logger.error(f"Error deleting log group {log_group}: {e}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='OpenSearch Serverless Migration Tool')
    
    parser.add_argument('--source-host', required=True, 
                        help='Source OpenSearch Serverless endpoint')
    parser.add_argument('--source-role', required=True, 
                        help='IAM role ARN for source ingestion')
    parser.add_argument('--target-host', required=True, 
                        help='Target OpenSearch Serverless endpoint')
    parser.add_argument('--target-role', required=True, 
                        help='IAM role ARN for target ingestion')
    parser.add_argument('--source-region', required=True, 
                        help='AWS region for source OpenSearch Serverless')
    parser.add_argument('--target-region', required=True, 
                        help='AWS region for target OpenSearch Serverless')
    parser.add_argument('--s3-bucket', required=True, 
                        help='S3 bucket name for intermediate storage')
    parser.add_argument('--max-concurrent', type=int, default=3, 
                        help='Maximum number of concurrent migrations')
    parser.add_argument('--tracker-file', default="migration_tracker.json",
                        help='Path to the migration tracker file (default: migration_tracker.json)')
    
    return parser.parse_args()


async def main_async():
    """Main async function to run the migration."""
    args = parse_args()
    
    config = {
        'source_host': args.source_host,
        'source_ingestion_role': args.source_role,
        'target_host': args.target_host,
        'target_ingestion_role': args.target_role,
        'source_region': args.source_region,
        'target_region': args.target_region,
        's3_bucket_name': args.s3_bucket,
        'migration_tracker_file': args.tracker_file
    }
    
    migration = OpenSearchMigration(config, max_concurrent=args.max_concurrent)
    
    try:
        logger.info("Starting OpenSearch Serverless migration...")
        created_pipelines, created_log_groups = await migration.migrate_indices()
        
        logger.info("Migration process completed.")
        
        # Print the migration summary
        migration.print_migration_summary()
        
        # # Ask user if they want to clean up resources
        # cleanup_choice = input("Do you want to clean up any remaining resources? (y/n): ")
        # if cleanup_choice.lower() == 'y':
        #     migration.cleanup_resources(created_pipelines, created_log_groups)
        #     logger.info("Cleanup completed.")
        # else:
        #     logger.info("Skipping cleanup. Resources will remain active.")
            
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        sys.exit(1)


def main():
    """Entry point for the script."""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user.")
        sys.exit(1)


if __name__ == "__main__":
    main()
    def generate_migration_summary(self) -> None:
        """
        Generate and update the final migration summary in the tracker file.
        This includes calculating the overall success rate and updating the summary section.
        """
        try:
            with open(self.migration_tracker_file, 'r') as f:
                tracker_data = json.load(f)
            
            # Count indices by status
            total = len(tracker_data["migration_status"])
            passed = sum(1 for entry in tracker_data["migration_status"] if entry.get("status") == "DONE")
            failed = sum(1 for entry in tracker_data["migration_status"] if entry.get("status") == "FAILED")
            ongoing = sum(1 for entry in tracker_data["migration_status"] if entry.get("status") == "ONGOING")
            
            # Calculate success rate
            success_rate = (passed / total * 100) if total > 0 else 0
            
            # Update summary
            tracker_data["summary"].update({
                "total": total,
                "passed": passed,
                "failed": failed,
                "ongoing": ongoing,
                "success_rate": f"{success_rate:.2f}%",
                "end_time": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat()
            })
            
            # Add overall result
            if ongoing == 0:
                if failed == 0:
                    tracker_data["summary"]["overall_result"] = "PASS"
                else:
                    tracker_data["summary"]["overall_result"] = "FAIL"
            else:
                tracker_data["summary"]["overall_result"] = "INCOMPLETE"
            
            # Write updated tracker
            with open(self.migration_tracker_file, 'w') as f:
                json.dump(tracker_data, f, indent=4)
                
            logger.info(f"Migration summary: Total={total}, Passed={passed}, Failed={failed}, Ongoing={ongoing}, Success Rate={success_rate:.2f}%")
            
        except Exception as e:
            logger.error(f"Error generating migration summary: {e}")
            
    def print_migration_summary(self) -> None:
        """
        Print a formatted summary of the migration results to the console.
        """
        try:
            with open(self.migration_tracker_file, 'r') as f:
                tracker_data = json.load(f)
            
            summary = tracker_data.get("summary", {})
            
            if not summary:
                logger.warning("No summary data available in the tracker file")
                return
                
            print("\n" + "="*80)
            print(f"{'OPENSEARCH SERVERLESS MIGRATION SUMMARY':^80}")
            print("="*80)
            
            print(f"Start Time: {summary.get('start_time', 'N/A')}")
            print(f"End Time: {summary.get('end_time', 'N/A')}")
            print(f"Overall Result: {summary.get('overall_result', 'N/A')}")
            print("-"*80)
            
            print(f"Total Indices: {summary.get('total', 0)}")
            print(f"Passed: {summary.get('passed', 0)}")
            print(f"Failed: {summary.get('failed', 0)}")
            print(f"Ongoing: {summary.get('ongoing', 0)}")
            print(f"Success Rate: {summary.get('success_rate', '0.00%')}")
            print("-"*80)
            
            # Print details of failed indices
            failed_indices = [entry for entry in tracker_data.get("migration_status", []) 
                             if entry.get("status") == "FAILED"]
            
            if failed_indices:
                print("\nFAILED INDICES:")
                for entry in failed_indices:
                    print(f"  - {entry.get('index')}: {entry.get('error', 'Unknown error')}")
            
            print("="*80 + "\n")
            
        except Exception as e:
            logger.error(f"Error printing migration summary: {e}")
            print("\nError: Could not print migration summary. See logs for details.")
