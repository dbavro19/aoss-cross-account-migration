from opensearchpy import OpenSearch
from opensearchpy import RequestsHttpConnection, OpenSearch, AWSV4SignerAuth
import boto3
import botocore
import time
import os
from pprint import pprint
import sys

# Build the client using the default credential configuration.
# You can use the CLI and run 'aws configure' to set access key, secret
# key, and default region.

host = sys.argv[1]
index_name = sys.argv[2]
#Setup Opensearch connectionand clinet
#host = os.environ["OPENSEARCH_ENDPOINT"] #use Opensearch Serverless host here
#host = "mqlz8a722dimi3bjkfsg.us-east-1.aoss.amazonaws.com"
#region = os.environ["AWS_DEFAULT_REGION"]# set region of you Opensearch severless collection
region = "us-east-1"
service = 'aoss'
credentials = boto3.Session().get_credentials() #Use enviroment credentials
auth = AWSV4SignerAuth(credentials, region, service) 
#oss_index = "git_try_1"

q_logs_oss_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)


def create_index(q_logs_oss_client):
    # index_body = {
    #     'settings': {
    #         'index': {
    #             'knn': True,
    #         }
    #     },
    #     'mappings': {
    #         'properties': {
    #             'text_embedding': {
    #                 'type': 'knn_vector',
    #                 'dimension': 768
    #             },
    #             'image_embedding': {
    #                 'type': 'knn_vector',
    #                 'dimension': 768
    #             }
    #         }
    #     }
    # }

    index_body = {
        "settings": {
            "index.knn": True
        },
        "mappings": {
            "properties": {
                "completionsVector": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                        "name": "hnsw",
                        #"space_type": "l2",  # Change to "l2" for Euclidean distance
                        "engine": "faiss",
                        # "parameters": {
                        #     "ef_construction": 512,
                        #     "m": 16,
                        #     "ef_search": 512
                        # }
                    }
                },
                "promptVector": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                        "name": "hnsw",
                        #"space_type": "l2",  # Change to "l2" for Euclidean distance
                        "engine": "faiss",
                        #"precision": "FP16",
                        # "parameters": {
                        #     "ef_construction": 512,
                        #     "m": 32,
                        #     "ef_search": 512
                        # }
                    }
                },
                "completions": {
                    "type": "text",
                },
                "conversation_id": {
                    "type": "text",
                },
                "customization_arn": {
                    "type": "text",
                },
                "event_type": {
                    "type": "text",
                },
                "file_name": {
                    "type": "text",
                },
                "follow_up_prompts": {
                    "type": "text",
                },
                "prompt": {
                    "type": "text",
                },
                "request_id": {
                    "type": "text",
                },
                "timestamp": {
                    "type": "date",
                },
                "user_id": {
                    "type": "text",
                },
                "user_name": {
                    "type": "text",
                },
            }
        }
    }
    pprint(index_body)
    response = q_logs_oss_client.indices.create(index=index_name, body=index_body)
    print('\nCreating index:')
    print(response)


if __name__ == "__main__":
    create_index(q_logs_oss_client)