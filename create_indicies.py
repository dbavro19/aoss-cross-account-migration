#!/usr/bin/env python3
"""
OpenSearch Serverless Index Creation Tool

This script creates indices in a target OpenSearch Serverless collection
using index definitions from a JSON file.
"""

import argparse
import json
import logging
import sys
from typing import Dict, List, Any, Optional, Tuple

import boto3
import requests
from requests_aws4auth import AWS4Auth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IndexCreator:
    """Class to handle OpenSearch Serverless index creation operations."""
    
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
        self.credentials = boto3.Session().get_credentials()
        
        # Initialize AWS4Auth for target OpenSearch API requests
        self.target_auth = AWS4Auth(
            self.credentials.access_key,
            self.credentials.secret_key,
            self.target_region,
            'aoss',
            session_token=self.credentials.token
        )
    
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
        Create an index in the target OpenSearch Serverless collection.
        
        Args:
            index_name: Name of the index to create
            index_body: Dictionary containing the mappings and settings
            
        Returns:
            API response as a dictionary
            
        Raises:
            Exception: If the API call fails
        """
        url = f"{self.target_host}/{index_name}"
        headers = {'Content-Type': 'application/json'}
        
        try:
            logger.info(f"Creating index: {index_name}")
            response = requests.put(
                url, 
                auth=self.target_auth, 
                data=json.dumps(index_body), 
                headers=headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating index {index_name}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
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
