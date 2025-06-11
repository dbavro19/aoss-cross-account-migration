#!/usr/bin/env python3
"""
OpenSearch Serverless Index Creation Tool using opensearch-py

This script creates indices in a target OpenSearch Serverless collection
using index definitions from a JSON file, leveraging the opensearch-py library.
"""

import argparse
import json
import logging
import sys
import urllib3
from typing import Dict, List, Any, Optional

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, ConnectionTimeout
from opensearchpy.exceptions import RequestError, ConnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class IndexCreator:
    """Class to handle OpenSearch Serverless index creation operations using opensearch-py."""
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the IndexCreator with configuration parameters.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.target_host = config['target_host']
        self.target_region = config['target_region']
        self.input_file = config['input_file']
        self.filter_indices = config.get('filter_indices', [])
        
        # Initialize AWS credentials
        credentials = boto3.Session().get_credentials()
        
        # Initialize AWSV4SignerAuth for OpenSearch Serverless
        auth = AWSV4SignerAuth(credentials, self.target_region, 'aoss')
        
        # Extract host without protocol
        if self.target_host.startswith('https://'):
            host = self.target_host[8:]
        else:
            host = self.target_host
        
        # Initialize OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=60,
            retry_on_timeout=True,
            max_retries=3
        )
        
        logger.info(f"Initialized OpenSearch client for host: {host}")
    
    def load_index_definitions(self) -> Dict[str, Any]:
        """
        Load index definitions from the input file.
        
        Returns:
            Dictionary mapping index names to their definitions
            
        Raises:
            Exception: If the file cannot be read or parsed
        """
        try:
            with open(self.input_file, 'r') as f:
                index_definitions = json.load(f)
            
            logger.info(f"Loaded {len(index_definitions)} index definitions from {self.input_file}")
            return index_definitions
        except Exception as e:
            logger.error(f"Error loading index definitions from {self.input_file}: {e}")
            raise
    
    def create_index(self, index_name: str, index_body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an index in the target OpenSearch Serverless collection using the low-level client.
        
        Args:
            index_name: Name of the index to create
            index_body: Dictionary containing the mappings and settings
            
        Returns:
            API response as a dictionary
            
        Raises:
            Exception: If the API call fails
        """
        try:
            logger.info(f"Creating index: {index_name}")
            
            # Use the low-level client approach
            response = self.client.indices.create(
                index=index_name,
                body=index_body,
                params={"format": "json"}  # Ensure JSON response format
            )
            
            logger.debug(f"Raw response for {index_name}: {response}")
            return response
        except RequestError as e:
            error_message = str(e)
            # Handle resource already exists error gracefully
            if "resource_already_exists_exception" in error_message:
                logger.warning(f"Index {index_name} already exists, skipping creation")
                return {"acknowledged": True, "status": "already_exists"}
            else:
                logger.error(f"Error creating index {index_name}: {e}")
                raise
    
    def create_all_indices(self) -> Dict[str, Any]:
        """
        Create indices in the target collection using definitions from the input file.
        
        Returns:
            Dictionary mapping index names to creation responses
        """
        results = {}
        
        try:
            # Load index definitions
            index_definitions = self.load_index_definitions()
            
            # Filter indices if specified
            if self.filter_indices:
                filtered_definitions = {k: v for k, v in index_definitions.items() if k in self.filter_indices}
                logger.info(f"Filtered {len(filtered_definitions)} indices from {len(index_definitions)} total")
                index_definitions = filtered_definitions
            
            for index_name, index_body in index_definitions.items():
                # Skip entries with error status
                if isinstance(index_body, dict) and 'status' in index_body and index_body['status'] == 'error':
                    logger.warning(f"Skipping index {index_name} due to extraction error: {index_body.get('error', 'Unknown error')}")
                    results[index_name] = {
                        'status': 'skipped',
                        'reason': f"Extraction error: {index_body.get('error', 'Unknown error')}"
                    }
                    continue
                
                try:
                    # Create the index in the target
                    response = self.create_index(index_name, index_body)
                    
                    # Store the result
                    results[index_name] = {
                        'status': 'success',
                        'response': response
                    }
                    logger.info(f"Successfully created index: {index_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to create index {index_name}: {e}")
                    results[index_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
            
            return results
            
        except Exception as e:
            logger.error(f"Error in create_all_indices: {e}")
            raise


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Create indices in a target OpenSearch Serverless collection using definitions from a file.'
    )
    
    parser.add_argument('--target-host', required=True,
                        help='Target OpenSearch Serverless endpoint URL')
    parser.add_argument('--target-region', required=True,
                        help='AWS region for the target OpenSearch Serverless collection')
    parser.add_argument('--input-file', default='index_definitions.json',
                        help='Input file with index definitions (default: index_definitions.json)')
    parser.add_argument('--indices', nargs='*',
                        help='Optional list of specific indices to create (default: all indices in the file)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    
    return parser.parse_args()


def main() -> None:
    """Main function to run the index creation process."""
    # Parse command line arguments
    args = parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create configuration dictionary
    config = {
        'target_host': args.target_host,
        'target_region': args.target_region,
        'input_file': args.input_file
    }
    
    # Add filter indices if specified
    if args.indices:
        config['filter_indices'] = args.indices
    
    try:
        # Initialize the IndexCreator
        creator = IndexCreator(config)
        
        # Create all indices
        results = creator.create_all_indices()
        
        # Print summary
        success_count = sum(1 for result in results.values() if result['status'] == 'success')
        error_count = sum(1 for result in results.values() if result['status'] == 'error')
        skipped_count = sum(1 for result in results.values() if result['status'] == 'skipped')
        
        logger.info(f"Index creation complete: {success_count} succeeded, {error_count} failed, {skipped_count} skipped")
        
        # Print detailed results if verbose
        if args.verbose:
            logger.debug("Detailed results:")
            logger.debug(json.dumps(results, indent=2))
        
        # Exit with error code if any indices failed
        if error_count > 0:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
