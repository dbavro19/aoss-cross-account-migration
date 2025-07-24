import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any

from base_migration import BaseMigration
from source_to_s3 import SourceToS3Migration
from s3_to_target import S3ToTargetMigration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MigrationOrchestrator(BaseMigration):
    """Class to orchestrate the full migration process from source to target."""
    
    def __init__(self, config_file: str):
        """
        Initialize the migration orchestrator.
        
        Args:
            config_file: Path to the JSON configuration file
        """
        super().__init__(config_file)
        self.config_file = config_file
    
    def _print_banner(self, message: str) -> None:
        """
        Print a banner message to make it stand out in the logs.
        
        Args:
            message: The message to display in the banner
        """
        banner_width = 80
        banner_char = "="
        
        logger.info("\n" + banner_char * banner_width)
        logger.info(message.center(banner_width))
        logger.info(banner_char * banner_width + "\n")
    
    async def migrate_index(self, index: str) -> Tuple[List[str], List[str]]:
        """
        Asynchronously migrate a single index from source to destination OpenSearch Serverless.
        
        Args:
            index: Name of the index to migrate
            
        Returns:
            Tuple containing lists of created pipeline names and log group names
        """
        created_pipelines = []
        created_log_groups = []
        
        # Print start banner
        self._print_banner(f"STARTING MIGRATION OF INDEX: {index}")
        
        start_time = datetime.now()
        
        try:
            # Step 1: Source to S3 migration
            self._print_banner(f"PHASE 1: MIGRATING {index} FROM SOURCE TO S3")
            source_to_s3 = SourceToS3Migration(self.config_file)
            s3_prefix, src_pipeline_name, migration_start_time = await source_to_s3.migrate_to_s3(index)
            
            created_pipelines.append((src_pipeline_name, False))  # (name, is_target)
            created_log_groups.append((self.src_pipeline_logs, False))
            
            # Step 2: S3 to Target migration
            self._print_banner(f"PHASE 2: MIGRATING {index} FROM S3 TO TARGET")
            s3_to_target = S3ToTargetMigration(self.config_file)
            dest_pipeline_name, target_migration_start_time = await s3_to_target.migrate_from_s3_to_target(
                index, 
                s3_prefix,
                self.source_host
            )
            
            created_pipelines.append((dest_pipeline_name, True))  # (name, is_target)
            created_log_groups.append((self.target_pipeline_logs, True))
            
            # Wait for data to settle
            logger.info(f"Waiting for data to settle for index {index}...")
            await asyncio.sleep(120)
            
            # Get source document count for verification
            loop = asyncio.get_event_loop()
            src_doc_count = await loop.run_in_executor(
                None,
                lambda: self.get_index_doc_count(self.source_host, index)
            )
            logger.info(f"Source document count for {index}: {src_doc_count}")
            
            # Clean up resources
            self._print_banner(f"PHASE 3: CLEANING UP RESOURCES FOR {index}")
            
            # Clean up source pipeline
            await source_to_s3.cleanup_source_pipeline(src_pipeline_name)
            self.update_tracker(index, checkpoint="DELETED_SOURCE_PIPELINE")
            
            # Clean up target pipeline
            await s3_to_target.cleanup_target_pipeline(dest_pipeline_name)
            self.update_tracker(index, checkpoint="DELETED_DESTINATION_PIPELINE")
            
            # Update status to DONE
            self.update_tracker(index, status="DONE", src_doc=src_doc_count)
            
            # Calculate elapsed time
            elapsed_time = datetime.now() - start_time
            hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            time_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            
            # Print completion banner
            self._print_banner(f"MIGRATION COMPLETED SUCCESSFULLY FOR INDEX: {index}")
            logger.info(f"Total migration time for {index}: {time_str}")
            logger.info(f"Documents migrated: {src_doc_count}")
        
        except Exception as e:
            # Calculate elapsed time even for failed migrations
            elapsed_time = datetime.now() - start_time
            hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            time_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
            
            # Print failure banner
            self._print_banner(f"MIGRATION FAILED FOR INDEX: {index}")
            logger.error(f"Error processing index {index}: {e}")
            logger.info(f"Time elapsed before failure: {time_str}")
            
            # Update status to FAILED
            self.update_tracker(index, status="FAILED", error=str(e))
        
        return created_pipelines, created_log_groups

    async def migrate_indices(self, skip_completed: bool = True) -> Tuple[List[Tuple[str, bool]], List[Tuple[str, bool]]]:
        """
        Asynchronously migrate indices from source OpenSearch Serverless to destination.
        
        Args:
            skip_completed: Whether to skip indices that have already been successfully migrated
            
        Returns:
            Tuple containing lists of created pipelines and log groups with their account flags
        """
        # Get Indices in the src opensearch Serverless
        logger.info('Getting indices from source OpenSearch Serverless...')
        indices = self.get_indices(self.source_host)
        
        if not indices:
            logger.warning("No indices found to migrate")
            return [], []
        
        # Check migration tracker to filter out already completed indices if requested
        indices_to_migrate = []
        if skip_completed and os.path.exists(self.migration_tracker_file):
            try:
                with open(self.migration_tracker_file, 'r') as f:
                    tracker_data = json.load(f)
                
                # Create a set of successfully migrated indices
                completed_indices = {
                    entry["index"] for entry in tracker_data.get("migration_status", [])
                    if entry.get("status") == "DONE"
                }
                
                # Filter out completed indices
                for index in indices:
                    if index in completed_indices:
                        logger.info(f"Skipping already migrated index: {index}")
                    else:
                        indices_to_migrate.append(index)
                        
                logger.info(f'Found {len(indices)} indices, {len(completed_indices)} already migrated, {len(indices_to_migrate)} to migrate')
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"Error reading migration tracker file: {e}. Will migrate all indices.")
                indices_to_migrate = indices
        else:
            indices_to_migrate = indices
            logger.info(f'Found {len(indices)} indices to migrate')
            
        if not indices_to_migrate:
            logger.info("All indices have already been successfully migrated")
            return [], []
            
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
        tasks = [limited_migrate_index(index) for index in indices_to_migrate]
        
        # Wait for all tasks to complete
        logger.info(f"Starting migration of {len(tasks)} indices with max {self.max_concurrent} concurrent tasks...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect resources for cleanup and handle exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Migration for index {indices_to_migrate[i]} failed with exception: {result}")
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
                    # Refresh credentials if needed for target operations
                    if is_target and self.source_account_id != self.target_account_id:
                        self.refresh_target_credentials_if_needed()
                        
                    self.delete_pipeline(pipeline_name, is_target)
                except Exception as e:
                    account = "target" if is_target else "source"
                    logger.error(f"Error deleting pipeline {pipeline_name} in {account} account: {e}")
        
        # Delete log groups
        for log_group, is_target in log_groups:
            try:
                # Refresh credentials if needed for target operations
                if is_target and self.source_account_id != self.target_account_id:
                    self.refresh_target_credentials_if_needed()
                    
                self.delete_log_group(log_group, is_target)
            except Exception as e:
                account = "target" if is_target else "source"
                logger.error(f"Error deleting log group {log_group} in {account} account: {e}")


async def main_async(config_file: str, skip_completed: bool = True):
    """
    Main async function to run the migration.
    
    Args:
        config_file: Path to the JSON configuration file
        skip_completed: Whether to skip indices that have already been successfully migrated
    """
    try:
        # Validate that the config file exists
        if not os.path.exists(config_file):
            logger.error(f"Configuration file not found: {config_file}")
            logger.info("Available configuration files:")
            for file in os.listdir('.'):
                if file.endswith('.json'):
                    logger.info(f"  - {file}")
            sys.exit(1)
            
        logger.info(f"Using configuration file: {config_file}")
        
        # Initialize the migration orchestrator
        migration = MigrationOrchestrator(config_file)
        
        logger.info("Starting OpenSearch Serverless cross-account migration...")
        created_pipelines, created_log_groups = await migration.migrate_indices(skip_completed=skip_completed)
        print(created_pipelines)
        print(created_log_groups)
        
        logger.info("Migration process completed.")
            
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed with error: {e}")
        sys.exit(1)


def main():
    """Entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='OpenSearch Serverless Cross-Account Migration Tool')
    parser.add_argument('--config', '-c', required=True, help='Path to the JSON configuration file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--force-all', '-f', action='store_true', help='Force migration of all indices, including already completed ones')
    
    args = parser.parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting OpenSearch Serverless Cross-Account Migration Tool")
    logger.info(f"Using configuration file: {args.config}")
    
    if args.force_all:
        logger.info("Force flag set: Will attempt to migrate all indices, including already completed ones")
    
    try:
        asyncio.run(main_async(args.config, not args.force_all))
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()