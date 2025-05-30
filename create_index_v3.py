#!/usr/bin/env python3
"""
OpenSearch Serverless Index Creation Tool

This script creates indices in a target OpenSearch Serverless collection
by copying the mappings and settings from a source collection.
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
        self.src_host = config['src_host']
        self.target_host = config['target_host']
        self.src_region = config['src_region']
        self.target_region = config['target_region']
        
        # Initialize AWS credentials
        self.credentials = boto3.Session().get_credentials()
        
        # Initialize AWS4Auth for source and target OpenSearch API requests
        self.src_auth = AWS4Auth(
            self.credentials.access_key,
            self.credentials.secret_key,
            self.src_region,
            'aoss',
            session_token=self.credentials.token
        )
        
        self.target_auth = AWS4Auth(
            self.credentials.access_key,
            self.credentials.secret_key,
            self.target_region,
            'aoss',
            session_token=self.credentials.token
        )
    
    def get_indices(self) -> List[str]:
        """
        Get list of indices from the source OpenSearch Serverless endpoint.
        
        Returns:
            List of index names
        
        Raises:
            Exception: If the API call fails
        """
        url = f"{self.src_host}/_cat/indices?format=json"
        try:
            response = requests.get(url, auth=self.src_auth)
            response.raise_for_status()
            indices = [doc['index'] for doc in response.json()]
            logger.info(f"Found {len(indices)} indices in source collection")
            return indices
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting indices: {e}")
            raise
    
    def get_index_definition(self, index_name: str) -> Dict[str, Any]:
        """
        Get the definition (mappings and settings) of an index from the source.
        
        Args:
            index_name: Name of the index
            
        Returns:
            Dictionary containing the index definition
            
        Raises:
            Exception: If the API call fails
        """
        url = f"{self.src_host}/{index_name}"
        try:
            response = requests.get(url, auth=self.src_auth)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting index definition for {index_name}: {e}")
            raise
    
    def extract_index_body(self, index_definition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract the relevant parts of the index definition for creating a new index.
        
        Args:
            index_definition: Full index definition from the source
            
        Returns:
            Dictionary containing the mappings and settings for the new index
        """
        index_body = {}
        
        for key, value in index_definition.items():
            if 'settings' in value:
                # Extract only the necessary settings
                new_settings = {
                    'index': {}
                }
                
                # Copy KNN settings if they exist
                if 'knn' in value['settings']['index']:
                    new_settings['index']['knn'] = value['settings']['index']['knn']
                
                # Copy shard and replica settings
                if 'number_of_replicas' in value['settings']['index']:
                    new_settings['index']['number_of_replicas'] = value['settings']['index']['number_of_replicas']
                
                if 'number_of_shards' in value['settings']['index']:
                    new_settings['index']['number_of_shards'] = value['settings']['index']['number_of_shards']
            
            # Copy mappings
            if 'mappings' in value:
                index_body['mappings'] = value['mappings']
                index_body['settings'] = new_settings
        
        return index_body
    
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
        Create all indices from the source in the target collection.
        
        Returns:
            Dictionary mapping index names to creation responses
        """
        results = {}
        
        try:
            # Get all indices from source
            indices = self.get_indices()
            
            for index in indices:
                try:
                    # Get index definition from source
                    index_definition = self.get_index_definition(index)
                    
                    # Extract the relevant parts for creating a new index
                    index_body = self.extract_index_body(index_definition)
                    
                    # Create the index in the target
                    response = self.create_index(index, index_body)
                    
                    # Store the result
                    results[index] = {
                        'status': 'success',
                        'response': response
                    }
                    logger.info(f"Successfully created index: {index}")
                    
                except Exception as e:
                    logger.error(f"Failed to create index {index}: {e}")
                    results[index] = {
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
        description='Create indices in a target OpenSearch Serverless collection by copying from a source.'
    )
    
    parser.add_argument('--src-host', required=True,
                        help='Source OpenSearch Serverless endpoint URL')
    parser.add_argument('--target-host', required=True,
                        help='Target OpenSearch Serverless endpoint URL')
    parser.add_argument('--src-region', required=True,
                        help='AWS region for the source OpenSearch Serverless collection')
    parser.add_argument('--target-region', required=True,
                        help='AWS region for the target OpenSearch Serverless collection')
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
        'src_host': args.src_host,
        'target_host': args.target_host,
        'src_region': args.src_region,
        'target_region': args.target_region
    }
    
    try:
        # Initialize the IndexCreator
        creator = IndexCreator(config)
        
        # Create all indices
        results = creator.create_all_indices()
        
        # Print summary
        success_count = sum(1 for result in results.values() if result['status'] == 'success')
        error_count = sum(1 for result in results.values() if result['status'] == 'error')
        
        logger.info(f"Index creation complete: {success_count} succeeded, {error_count} failed")
        
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
