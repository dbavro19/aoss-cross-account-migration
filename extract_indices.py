#!/usr/bin/env python3
"""
OpenSearch Serverless Index Extraction Tool

This script extracts index definitions from a source OpenSearch Serverless collection
and saves them to a JSON file for later use.
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


class IndexExtractor:
    """Class to handle OpenSearch Serverless index extraction operations."""
    
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the IndexExtractor with configuration parameters.
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.src_host = config['src_host']
        self.src_region = config['src_region']
        self.output_file = config['output_file']
        
        # Initialize AWS credentials
        self.credentials = boto3.Session().get_credentials()
        
        # Initialize AWS4Auth for source OpenSearch API requests
        self.src_auth = AWS4Auth(
            self.credentials.access_key,
            self.credentials.secret_key,
            self.src_region,
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
    
    def extract_all_indices(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract all index definitions from the source and save to a file.
        
        Returns:
            Dictionary mapping index names to their definitions
        """
        index_definitions = {}
        
        try:
            # Get all indices from source
            indices = self.get_indices()
            
            for index in indices:
                try:
                    # Get index definition from source
                    index_definition = self.get_index_definition(index)
                    
                    # Extract the relevant parts for creating a new index
                    index_body = self.extract_index_body(index_definition)
                    
                    # Store the result
                    index_definitions[index] = index_body
                    logger.info(f"Successfully extracted index definition: {index}")
                    
                except Exception as e:
                    logger.error(f"Failed to extract index definition for {index}: {e}")
                    index_definitions[index] = {
                        'status': 'error',
                        'error': str(e)
                    }
            
            # Save to file
            with open(self.output_file, 'w') as f:
                json.dump(index_definitions, f, indent=2)
            
            logger.info(f"Saved {len(index_definitions)} index definitions to {self.output_file}")
            
            return index_definitions
            
        except Exception as e:
            logger.error(f"Error in extract_all_indices: {e}")
            raise


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Extract index definitions from a source OpenSearch Serverless collection.'
    )
    
    parser.add_argument('--src-host', required=True,
                        help='Source OpenSearch Serverless endpoint URL')
    parser.add_argument('--src-region', required=True,
                        help='AWS region for the source OpenSearch Serverless collection')
    parser.add_argument('--output-file', default='index_definitions.json',
                        help='Output file to save index definitions (default: index_definitions.json)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    
    return parser.parse_args()


def main() -> None:
    """Main function to run the index extraction process."""
    # Parse command line arguments
    args = parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create configuration dictionary
    config = {
        'src_host': args.src_host,
        'src_region': args.src_region,
        'output_file': args.output_file
    }
    
    try:
        # Initialize the IndexExtractor
        extractor = IndexExtractor(config)
        
        # Extract all indices
        index_definitions = extractor.extract_all_indices()
        
        # Print summary
        success_count = sum(1 for def_dict in index_definitions.values() if 'status' not in def_dict or def_dict['status'] != 'error')
        error_count = sum(1 for def_dict in index_definitions.values() if 'status' in def_dict and def_dict['status'] == 'error')
        
        logger.info(f"Index extraction complete: {success_count} succeeded, {error_count} failed")
        
        # Exit with error code if any indices failed
        if error_count > 0:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
