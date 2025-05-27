import boto3
from pprint import pprint
import requests
import sys
from requests_aws4auth import AWS4Auth
import time
import json
import os
import asyncio
import concurrent.futures
from datetime import datetime

src_host = sys.argv[1]
src_ingestion_role = sys.argv[2]
dest_host = sys.argv[3]
dest_ingestion_role = sys.argv[4]

region = "us-east-1"
credentials = boto3.Session().get_credentials()
pprint(credentials)
pipeline_client = boto3.client('osis', region_name=region)
logs_client = boto3.client('logs', region_name=region)

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    'aoss',
    session_token=credentials.token
)

def get_indices(host):
    url = f"{host}/_cat/indices?format=json"
    response = requests.get(url, auth=awsauth)
    return response

def get_index(host, index_name):
    url = f"{host}/{index_name}"
    response = requests.get(url, auth=awsauth)
    return response

def get_index_doc_count(host, index_name):
    #url = f"{host}/_cat/count?format=json"
    #url = f"{host}/{index_name}"
    url = f"{host}/_cat/indices/{index_name}?format=json"
    response = requests.get(url, auth=awsauth)
    # print(dir(response))
    # pprint(response.json())
    return response.json()[0]['docs.count']

def create_src_ingestion_pipeline(pipeline_name, source_opensearch_endpoint, 
                             s3_bucket_name, index_name, s3_prefix, iam_role, log_group_name, region="us-east-1"):
    
    #log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{pipeline_name}'

    try:
        logs_client.create_log_group(logGroupName=log_group_name)
        print(f"Created log group: {log_group_name}")
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"Log group already exists: {log_group_name}")
        use_logging = True
    except Exception as e:
        print(f"Error creating log group: {e}")
        # If we can't create the log group, disable logging
        print("Disabling logging for pipeline")
        use_logging = False
    else:
        use_logging = True
        
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
                    region: {region}
                    sts_role_arn: {iam_role}
                indices:
                    include:
                        - index_name_regex: {index_name}
        sink:
            - s3:
                bucket: {s3_bucket_name}
                object_key:
                    path_prefix: {s3_prefix}
                compression: none
                codec:
                    json: {{}}
                threshold:
                    event_collect_timeout: 300s
                aws:
                    region: {region}
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
        response = pipeline_client.create_pipeline(**pipeline_args)
        return response
    except Exception as e:
        print(f"Error creating pipeline: {e}")
        print(f"Pipeline configuration used:\n{pipeline_body}")
        raise

def create_dest_ingestion_pipeline(pipeline_name, opensearch_endpoint, 
                             s3_bucket_name, s3_prefix, index_name, iam_role, log_group_name, region="us-east-1"):
    
    #log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{pipeline_name}'

    try:
        logs_client.create_log_group(logGroupName=log_group_name)
        print(f"Created log group: {log_group_name}")
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"Log group already exists: {log_group_name}")
        use_logging = True
    except Exception as e:
        print(f"Error creating log group: {e}")
        # If we can't create the log group, disable logging
        print("Disabling logging for pipeline")
        use_logging = False
    else:
        use_logging = True

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
                            name: {s3_bucket_name}
                            filter:
                                include_prefix: 
                                    - {s3_prefix}
                aws:
                    region: {region}
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
                    region: {region}
                    sts_role_arn: {iam_role}
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
        response = pipeline_client.create_pipeline(**pipeline_args)
        return response
    except Exception as e:
        print(f"Error creating pipeline: {e}")
        print(f"Pipeline configuration used:\n{pipeline_body}")
        raise

def wait_for_pipeline_active(pipeline_name, max_attempts=30, delay_seconds=10):
    """
    Poll the pipeline status until it becomes ACTIVE or reaches max attempts
    
    Args:
        pipeline_name (str): Name of the pipeline to monitor
        max_attempts (int): Maximum number of polling attempts
        delay_seconds (int): Delay between polling attempts in seconds
        
    Returns:
        bool: True if pipeline is active, False otherwise
    """
    
    print(f"Waiting for pipeline '{pipeline_name}' to become active...")
    
    for attempt in range(1, max_attempts + 1):
        try:
            response = pipeline_client.get_pipeline(PipelineName=pipeline_name)
            # pprint(response)
            status = response['Pipeline'].get('Status')
            
            print(f"Attempt {attempt}/{max_attempts}: Pipeline: {pipeline_name} status: {status}")
            
            if status == 'ACTIVE':
                print(f"Pipeline '{pipeline_name}' is now ACTIVE!")
                return True
            elif status in ['FAILED', 'DELETED']:
                print(f"Pipeline reached terminal state: {status}")
                return False
                
            # Wait before next check
            time.sleep(delay_seconds)
            
        except Exception as e:
            print(f"Error checking pipeline status: {e}")
            time.sleep(delay_seconds)
    
    print(f"Timed out waiting for pipeline '{pipeline_name}' to become active")
    return False

def delete_pipeline(pipeline_name):
    """
    Delete an OpenSearch Ingestion Pipeline
    
    Args:
        pipeline_name (str): Name of the pipeline to delete
    """
    try:
        response = pipeline_client.delete_pipeline(PipelineName=pipeline_name)
        print(f"Successfully deleted pipeline: {pipeline_name}")
        return response
    except Exception as e:
        print(f"Error deleting pipeline: {e}")
        raise

# def make_index_readonly(host, index_name):
#     """
#     Make an index read-only by updating its settings
    
#     Args:
#         host (str): OpenSearch endpoint
#         index_name (str): Name of the index
#     """
#     url = f"{host}/{index_name}/_settings"
#     settings = {
#         "index": {
#             "blocks": {
#                 "write": False
#             }
#         }
#     }
#     response = requests.put(url, json=settings, auth=awsauth)
#     if response.status_code == 200:
#         print(f"Successfully made index {index_name} read-only")
#     else:
#         print(f"Failed to make index {index_name} read-only: {response.text}")
#     return response


def delete_log_group(log_group_name):
    """
    Delete a CloudWatch log group

    Args:
        log_group_name (str): Name of the log group to delete
    """
    try:
        logs_client.delete_log_group(logGroupName=log_group_name)
        print(f"Successfully deleted log group: {log_group_name}")
    except Exception as e:
        print(f"Error deleting log group {log_group_name}: {e}")

def cleanup_resources(pipeline_names, log_groups, delete_pipelines=True):
    """
    Clean up all resources created during migration
    
    Args:
        pipeline_names (list): List of pipeline names to delete
        log_groups (list): List of CloudWatch log groups to delete
    """
    print("Starting cleanup process...")
    
    # Delete pipelines
    if delete_pipelines:
        for pipeline_name in pipeline_names:
            try:
                delete_pipeline(pipeline_name)
            except Exception as e:
                print(f"Error deleting pipeline {pipeline_name}: {e}")
    
    # Delete log groups
    for log_group in log_groups:
        try:
            logs_client.delete_log_group(logGroupName=log_group)
            print(f"Successfully deleted log group: {log_group}")
        except Exception as e:
            print(f"Error deleting log group {log_group}: {e}")

# def update_migration_status(index_name, metrics, status="ongoing"):
#     """
#     Update the migration status JSON file with the latest information
    
#     Args:
#         index_name (str): Name of the index being migrated
#         metrics (dict): Migration metrics for the index
#         status (str): Status of the migration ("ongoing" or "completed")
#     """
#     status_file = "migration_status.json"
    
#     # Read existing status
#     try:
#         with open(status_file, 'r') as f:
#             status_data = json.load(f)
#     except FileNotFoundError:
#         status_data = {
#             "migration_status": {
#                 "completed_indices": [],
#                 "ongoing_indices": [],
#                 "last_updated": "",
#                 "migration_stats": {}
#             }
#         }
    
#     # Update status
#     if status == "completed":
#         if index_name in status_data["migration_status"]["ongoing_indices"]:
#             status_data["migration_status"]["ongoing_indices"].remove(index_name)
#         if index_name not in status_data["migration_status"]["completed_indices"]:
#             status_data["migration_status"]["completed_indices"].append(index_name)
#     elif status == "ongoing":
#         if index_name not in status_data["migration_status"]["ongoing_indices"]:
#             status_data["migration_status"]["ongoing_indices"].append(index_name)
    
#     # Update metrics
#     status_data["migration_status"]["migration_stats"][index_name] = metrics
#     status_data["migration_status"]["last_updated"] = datetime.now()
    
#     # Write updated status
#     with open(status_file, 'w') as f:
#         json.dump(status_data, f, indent=4)


# def get_pipelines():
#     pipelines = []
#     try:
#         response = pipeline_client.list_pipelines()
#         for data in response['Pipelines']:
#             #pprint(data)
#             pipelines.append(data['PipelineName'])
#         return pipelines
#     except Exception as e:
#         print(f"Error returing pipeline: {e}")
#         raise

# def delete_pipelines(pipelines):
#     for pipe in pipelines:
#         try:
#             response = pipeline_client.delete_pipeline(PipelineName=pipe)
#             print(f"Successfully deleted pipeline: {pipe}")
#             print(response)
#         except Exception as e:
#             print(f"Error deleting pipeline {pipe}: {e}")


def update_tracker(tracker_file, index, checkpoint=None, status=None, error=None, src_doc=None, dest_doc=None):
    """
    Update the migration tracker file for a specific index
    
    Args:
        tracker_file (str): Path to the tracker file
        index (str): Name of the index being processed
        checkpoint (str, optional): Current checkpoint in the migration process
        status (str, optional): Current status of the migration
        error (str, optional): Error message if migration failed
        src_doc (int, optional): Document count in source
        dest_doc (int, optional): Document count in destination
    """
    with open(tracker_file, 'r') as f:
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
    with open(tracker_file, 'w') as f:
        json.dump(tracker_data, f, indent=4)

def migrate_opensearch_indices(src_host, src_ingestion_role, dest_host, dest_ingestion_role, s3_bucket_name="auto-test-12345", region="us-east-1"):
    """
    Migrate indices from source OpenSearch Serverless to destination OpenSearch Serverless
    
    Args:
        src_host (str): Source OpenSearch Serverless endpoint
        src_ingestion_role (str): IAM role ARN for source ingestion
        dest_host (str): Destination OpenSearch Serverless endpoint
        dest_ingestion_role (str): IAM role ARN for destination ingestion
        s3_bucket_name (str): S3 bucket name for intermediate storage
        region (str): AWS region
        
    Returns:
        tuple: Lists of created pipelines and log groups for cleanup
    """
    # Create a json file with name migration_tracker
    migration_tracker_file = "migration_tracker.json"
    
    # Initialize migration tracker if it doesn't exist
    if not os.path.exists(migration_tracker_file):
        with open(migration_tracker_file, 'w') as f:
            json.dump({"migration_status": []}, f, indent=4)
    
    # Get Indices in the src opensearch Serverless
    print('Getting indices from source OpenSearch Serverless...')
    response = get_indices(src_host)
    indices = []
    
    if response.status_code != 200:
        print(f"Error getting indices: {response.text}")
        return [], []
        
    for doc in response.json():
        indices.append(doc['index'])
    
    print('#'*30)
    print(f'Found indices: {indices}')
    print('#'*30)
    
    # Track resources for cleanup
    created_pipelines = []
    created_log_groups = []
    
    # Loop over the indices
    for index in indices:
        print('#'*30)
        print(f'Processing index: {index}')
        print('#'*30)
        
        # 1. Create a json structure for tracking this index
        update_tracker(
            migration_tracker_file, 
            index, 
            checkpoint="CREATING_SOURCE_PIPELINE", 
            status="ONGOING"
        )
        
        # # Make source index read-only to prevent data changes during migration
        # print(f"Making source index {index} read-only...")
        # make_index_readonly(src_host, index)
        
        # 2. Create a pipeline to move data from source opensearch serverless to S3
        pipeline_index = index.replace('_', '-')
        s3_prefix = f"src-{pipeline_index}/"
        src_pipeline_name = f"oss-s3-pipe-{pipeline_index}"
        src_log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{src_pipeline_name}'
        
        created_pipelines.append(src_pipeline_name)
        created_log_groups.append(src_log_group_name)
        
        try:
            print(f"Creating source pipeline: {src_pipeline_name}")
            src_response = create_src_ingestion_pipeline(
                pipeline_name=src_pipeline_name, 
                source_opensearch_endpoint=src_host, 
                s3_bucket_name=s3_bucket_name, 
                index_name=index,
                s3_prefix=s3_prefix, 
                iam_role=src_ingestion_role,
                log_group_name=src_log_group_name, 
                region=region)
            
            # Update checkpoint in tracker
            update_tracker(migration_tracker_file, index, checkpoint="CREATING_SOURCE_PIPELINE")
            
            # 3. Wait for pipeline to be active
            if not wait_for_pipeline_active(src_pipeline_name, max_attempts=30, delay_seconds=10):
                raise Exception(f"Source pipeline {src_pipeline_name} failed to become active")
            
            # Wait for 2 minutes as specified
            print(f"Waiting 3 minutes for data transfer to s3 to complete...")
            time.sleep(180)
            
            # 4. Create a pipeline to move data from S3 to destination opensearch serverless
            dest_pipeline_name = f"s3-oss-pipe-{pipeline_index}"
            dest_log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{dest_pipeline_name}'
            
            created_pipelines.append(dest_pipeline_name)
            created_log_groups.append(dest_log_group_name)
            
            # Update checkpoint in tracker
            update_tracker(migration_tracker_file, index, checkpoint="CREATING_DESTINANTION_PIPELINE")
            
            print(f"Creating destination pipeline: {dest_pipeline_name}")
            dest_response = create_dest_ingestion_pipeline(
                pipeline_name=dest_pipeline_name,
                opensearch_endpoint=dest_host,
                s3_bucket_name=s3_bucket_name,
                s3_prefix=s3_prefix,
                index_name=index,
                iam_role=dest_ingestion_role,
                log_group_name=dest_log_group_name, 
                region=region)
            
            # 5. Wait for pipeline to be active and then wait 2 minutes
            if not wait_for_pipeline_active(dest_pipeline_name, max_attempts=30, delay_seconds=10):
                raise Exception(f"Destination pipeline {dest_pipeline_name} failed to become active")
            
            print(f"Waiting 3 minutes for data transfer to destination oss to complete...")
            time.sleep(180)
            
            # 6. Gather document count from source and destination
            src_doc_count = int(get_index_doc_count(src_host, index))
            
            # Check if destination index exists
            dest_indices_response = get_indices(dest_host)
            dest_indices = [doc['index'] for doc in dest_indices_response.json()]
            
            if index in dest_indices:
                dest_doc_count = int(get_index_doc_count(dest_host, index))
            # else:
            #     dest_doc_count = 0

            # dest_doc_count = int(get_index_doc_count(dest_host, index))
            
            print(f"Source document count: {src_doc_count}")
            print(f"Destination document count: {dest_doc_count}")
            
            # 7. If document count is same
            if src_doc_count == dest_doc_count and src_doc_count > 0:
                print('#'*30)
                print("src docs and dest docs are same")
                print('#'*30)
                # Delete the source pipeline
                print(f"Deleting source pipeline: {src_pipeline_name}")
                delete_pipeline(src_pipeline_name)
                print(f"Deleting source pipeline log group pipeline: {src_log_group_name}")
                delete_log_group(src_log_group_name)
                
                # Update checkpoint in tracker
                update_tracker(migration_tracker_file, index, checkpoint="DELETED_SOURCE_PIPELINE")
                
                # Delete the destination pipeline
                print(f"Deleting destination pipeline: {dest_pipeline_name}")
                delete_pipeline(dest_pipeline_name)
                print(f"Deleting destination pipeline log group pipeline: {dest_log_group_name}")
                delete_log_group(dest_log_group_name)
                
                # Update checkpoint in tracker
                update_tracker(migration_tracker_file, index, checkpoint="DELETED_DESTINATION_PIPELINE")
                
                # Update status to DONE
                update_tracker(migration_tracker_file, index, status="DONE", src_doc=src_doc_count, dest_doc=dest_doc_count)
                
                print(f"Migration completed successfully for index {index}")
                
            # 8. If document count is not equal
            else:
                # Update status to FAILED
                update_tracker(
                    migration_tracker_file, 
                    index, 
                    status="FAILED", 
                    src_doc=src_doc_count, 
                    dest_doc=dest_doc_count
                )
                
                print(f"Migration failed for index {index}. Document counts don't match.")
                print(f"Source documents: {src_doc_count}")
                print(f"Destination documents: {dest_doc_count}")
        
        except Exception as e:
            print(f"Error processing index {index}: {e}")
            
            # Update status to FAILED
            update_tracker(migration_tracker_file, index, status="FAILED", error=str(e))
    
    return created_pipelines, created_log_groups


async def migrate_index(index, src_host, src_ingestion_role, dest_host, dest_ingestion_role, 
                       s3_bucket_name, migration_tracker_file, region="us-east-1"):
    """
    Asynchronously migrate a single index from source to destination OpenSearch Serverless
    
    Args:
        index (str): Name of the index to migrate
        src_host (str): Source OpenSearch Serverless endpoint
        src_ingestion_role (str): IAM role ARN for source ingestion
        dest_host (str): Destination OpenSearch Serverless endpoint
        dest_ingestion_role (str): IAM role ARN for destination ingestion
        s3_bucket_name (str): S3 bucket name for intermediate storage
        migration_tracker_file (str): Path to the migration tracker file
        region (str): AWS region
        
    Returns:
        tuple: Created pipeline names and log group names for cleanup
    """
    created_pipelines = []
    created_log_groups = []
    
    print(f"Processing index: {index}")
    
    # 1. Create a json structure for tracking this index
    update_tracker(
        migration_tracker_file, 
        index, 
        checkpoint="CREATING_SOURCE_PIPELINE", 
        status="ONGOING"
    )
    
    try:
        # Make source index read-only to prevent data changes during migration
        print(f"Making source index {index} read-only...")
        # Use asyncio.to_thread for blocking operations
        loop = asyncio.get_event_loop()
        # await loop.run_in_executor(None, lambda: make_index_readonly(src_host, index))
        
        # 2. Create a pipeline to move data from source opensearch serverless to S3
        pipeline_index = index.replace('_', '-')
        s3_prefix = f"src-{pipeline_index}/"
        src_pipeline_name = f"oss-s3-pipe-{pipeline_index}"
        src_log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{src_pipeline_name}'
        
        created_pipelines.append(src_pipeline_name)
        created_log_groups.append(src_log_group_name)
        
        print(f"Creating source pipeline: {src_pipeline_name}")
        # Use asyncio.to_thread for blocking operations
        src_response = await loop.run_in_executor(
            None, 
            lambda: create_src_ingestion_pipeline(
                pipeline_name=src_pipeline_name, 
                source_opensearch_endpoint=src_host, 
                s3_bucket_name=s3_bucket_name, 
                index_name=index,
                s3_prefix=s3_prefix, 
                iam_role=src_ingestion_role,
                log_group_name=src_log_group_name, 
                region=region
            )
        )
        
        # Update checkpoint in tracker
        update_tracker(migration_tracker_file, index, checkpoint="CREATING_SOURCE_PIPELINE")
        
        # 3. Wait for pipeline to be active
        pipeline_active = await loop.run_in_executor(
            None,
            lambda: wait_for_pipeline_active(src_pipeline_name, max_attempts=40, delay_seconds=10)
        )
        
        if not pipeline_active:
            raise Exception(f"Source pipeline {src_pipeline_name} failed to become active")
        
        # Wait for 2 minutes as specified
        print(f"Waiting 3 minutes for data transfer to s3 to complete for index {index}...")
        await asyncio.sleep(180)
        
        # 4. Create a pipeline to move data from S3 to destination opensearch serverless
        dest_pipeline_name = f"s3-oss-pipe-{pipeline_index}"
        dest_log_group_name = f'/aws/vendedlogs/OpenSearchIngestion/{dest_pipeline_name}'
        
        created_pipelines.append(dest_pipeline_name)
        created_log_groups.append(dest_log_group_name)
        
        # Update checkpoint in tracker
        update_tracker(migration_tracker_file, index, checkpoint="CREATING_DESTINANTION_PIPELINE")
        
        print(f"Creating destination pipeline: {dest_pipeline_name}")
        dest_response = await loop.run_in_executor(
            None,
            lambda: create_dest_ingestion_pipeline(
                pipeline_name=dest_pipeline_name,
                opensearch_endpoint=dest_host,
                s3_bucket_name=s3_bucket_name,
                s3_prefix=s3_prefix,
                index_name=index,
                iam_role=dest_ingestion_role,
                log_group_name=dest_log_group_name, 
                region=region
            )
        )
        
        # 5. Wait for pipeline to be active and then wait 2 minutes
        pipeline_active = await loop.run_in_executor(
            None,
            lambda: wait_for_pipeline_active(dest_pipeline_name, max_attempts=40, delay_seconds=10)
        )
        
        if not pipeline_active:
            raise Exception(f"Destination pipeline {dest_pipeline_name} failed to become active")
        
        print(f"Waiting 3 minutes for data transfer to destination oss to complete for index {index}...")
        await asyncio.sleep(180)
        
        # 6. Gather document count from source and destination
        src_doc_count = await loop.run_in_executor(
            None,
            lambda: int(get_index_doc_count(src_host, index))
        )
        
        # Check if destination index exists
        dest_indices_response = await loop.run_in_executor(
            None,
            lambda: get_indices(dest_host)
        )
        
        dest_indices = [doc['index'] for doc in dest_indices_response.json()]
        
        if index in dest_indices:
            dest_doc_count = await loop.run_in_executor(
                None,
                lambda: int(get_index_doc_count(dest_host, index))
            )
        else:
            dest_doc_count = 0
        
        print(f"Source document count for {index}: {src_doc_count}")
        print(f"Destination document count for {index}: {dest_doc_count}")
        
        # 7. If document count is same
        if src_doc_count == dest_doc_count and src_doc_count > 0:
            print('#'*30)
            print("src docs and dest docs are same")
            print('#'*30)

            # Delete the source pipeline
            print(f"Deleting source pipeline: {src_pipeline_name}")
            await loop.run_in_executor(None, lambda: delete_pipeline(src_pipeline_name))
            print(f"Deleting source pipeline log group pipeline: {src_log_group_name}")
            await loop.run_in_executor(None, lambda: delete_log_group(src_log_group_name))

            # Update checkpoint in tracker
            update_tracker(migration_tracker_file, index, checkpoint="DELETED_SOURCE_PIPELINE")

            # Delete the destination pipeline
            print(f"Deleting destination pipeline: {dest_pipeline_name}")
            await loop.run_in_executor(None, lambda: delete_pipeline(dest_pipeline_name))
            print(f"Deleting destination pipeline log group pipeline: {dest_log_group_name}")
            await loop.run_in_executor(None, lambda: delete_log_group(dest_log_group_name))


            # Update checkpoint in tracker
            update_tracker(migration_tracker_file, index, checkpoint="DELETED_DESTINATION_PIPELINE")
            
            # Update status to DONE
            update_tracker(migration_tracker_file, index, status="DONE", src_doc=src_doc_count, dest_doc=dest_doc_count)
            
            print(f"Migration completed successfully for index {index}")
            
        # 8. If document count is not equal
        else:
            # Update status to FAILED
            update_tracker(
                migration_tracker_file, 
                index, 
                status="FAILED", 
                src_doc=src_doc_count, 
                dest_doc=dest_doc_count
            )
            
            print(f"Migration failed for index {index}. Document counts don't match.")
            print(f"Source documents: {src_doc_count}")
            print(f"Destination documents: {dest_doc_count}")
    
    except Exception as e:
        print(f"Error processing index {index}: {e}")
        
        # Update status to FAILED
        update_tracker(migration_tracker_file, index, status="FAILED", error=str(e))
    
    return created_pipelines, created_log_groups

async def migrate_opensearch_indices_async(src_host, src_ingestion_role, dest_host, dest_ingestion_role, 
                                          s3_bucket_name="auto-test-12345", max_concurrent=3, region="us-east-1"):
    """
    Asynchronously migrate indices from source OpenSearch Serverless to destination OpenSearch Serverless
    
    Args:
        src_host (str): Source OpenSearch Serverless endpoint
        src_ingestion_role (str): IAM role ARN for source ingestion
        dest_host (str): Destination OpenSearch Serverless endpoint
        dest_ingestion_role (str): IAM role ARN for destination ingestion
        s3_bucket_name (str): S3 bucket name for intermediate storage
        max_concurrent (int): Maximum number of indices to migrate concurrently
        region (str): AWS region
        
    Returns:
        tuple: Lists of created pipelines and log groups for cleanup
    """
    # Create a json file with name migration_tracker
    migration_tracker_file = "migration_tracker.json"
    
    # Initialize migration tracker if it doesn't exist
    if not os.path.exists(migration_tracker_file):
        with open(migration_tracker_file, 'w') as f:
            json.dump({"migration_status": []}, f, indent=4)
    
    # Get Indices in the src opensearch Serverless
    print('Getting indices from source OpenSearch Serverless...')
    response = get_indices(src_host)
    indices = []
    
    if response.status_code != 200:
        print(f"Error getting indices: {response.text}")
        return [], []
        
    for doc in response.json():
        indices.append(doc['index'])
    
    print('#'*30)
    print(f'Found {len(indices)} indices: {indices}')
    print(f'Processing with max_concurrent={max_concurrent}')
    print('#'*30)
    
    # Track resources for cleanup
    all_created_pipelines = []
    all_created_log_groups = []
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def limited_migrate_index(index):
        """Wrapper to limit concurrent executions with semaphore"""
        async with semaphore:
            print(f"Starting migration for index: {index}")
            return await migrate_index(
                index=index,
                src_host=src_host,
                src_ingestion_role=src_ingestion_role,
                dest_host=dest_host,
                dest_ingestion_role=dest_ingestion_role,
                s3_bucket_name=s3_bucket_name,
                migration_tracker_file=migration_tracker_file,
                region=region
            )
    
    # Create tasks for all indices at once, but limit concurrent execution with semaphore
    tasks = [limited_migrate_index(index) for index in indices]
    
    # Wait for all tasks to complete
    print(f"Starting migration of {len(tasks)} indices with max {max_concurrent} concurrent tasks...")
    results = await asyncio.gather(*tasks)
    
    # Collect resources for cleanup
    for pipelines, log_groups in results:
        all_created_pipelines.extend(pipelines)
        all_created_log_groups.extend(log_groups)
    
    return all_created_pipelines, all_created_log_groups

def migrate_opensearch_indices_async_wrapper(src_host, src_ingestion_role, dest_host, dest_ingestion_role, 
                                           s3_bucket_name="auto-test-12345", max_concurrent=3, region="us-east-1"):
    """
    Wrapper function to run the async migration function in an event loop
    
    Args:
        src_host (str): Source OpenSearch Serverless endpoint
        src_ingestion_role (str): IAM role ARN for source ingestion
        dest_host (str): Destination OpenSearch Serverless endpoint
        dest_ingestion_role (str): IAM role ARN for destination ingestion
        s3_bucket_name (str): S3 bucket name for intermediate storage
        max_concurrent (int): Maximum number of indices to migrate concurrently
        region (str): AWS region
        
    Returns:
        tuple: Lists of created pipelines and log groups for cleanup
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        migrate_opensearch_indices_async(
            src_host=src_host,
            src_ingestion_role=src_ingestion_role,
            dest_host=dest_host,
            dest_ingestion_role=dest_ingestion_role,
            s3_bucket_name=s3_bucket_name,
            max_concurrent=max_concurrent,
            region=region
        )
    )

if __name__ == "__main__":
    # Ask user which migration method to use
    print("Choose migration method:")
    print("1. Sequential migration (one index at a time)")
    print("2. Async migration (multiple indices in parallel)")
    
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == "1":
        # Execute the sequential migration process
        created_pipelines, created_log_groups = migrate_opensearch_indices(
            src_host=src_host,
            src_ingestion_role=src_ingestion_role,
            dest_host=dest_host,
            dest_ingestion_role=dest_ingestion_role
        )
    else:
        # Execute the async migration process
        max_concurrent = input("Enter maximum number of concurrent migrations (default: 3): ")
        try:
            max_concurrent = int(max_concurrent)
        except (ValueError, TypeError):
            max_concurrent = 3
        
        print(f"Starting async migration with max {max_concurrent} concurrent indices...")
        created_pipelines, created_log_groups = migrate_opensearch_indices_async_wrapper(
            src_host=src_host,
            src_ingestion_role=src_ingestion_role,
            dest_host=dest_host,
            dest_ingestion_role=dest_ingestion_role,
            max_concurrent=max_concurrent
        )
    
    # # Ask user if they want to clean up resources
    # cleanup_choice = input("Do you want to clean up created resources? (y/n): ")
    # if cleanup_choice.lower() == 'y':
    #     cleanup_resources(created_pipelines, created_log_groups)
    #     print("Cleanup completed.")
    # else:
    #     print("Skipping cleanup. Resources will remain active.")