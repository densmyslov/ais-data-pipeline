import json
import boto3
import os
from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct

class DubaiDatasetStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load configuration files
        with open('config/parameters.json', 'r') as f:
            params = json.load(f)
        
        with open('config/secrets.json', 'r') as f:
            secrets = json.load(f)

        # ===============Create an S3 bucket for data storage================
        # Use the bucket name from parameters.json + suffix: {account_id}-{region}
        base_bucket_name = params.get('bucket_name', 'dubai-real-estate-data')
        account_id = secrets.get('bucket_suffix', '')  # AWS Account ID from secrets
        
        # Get region from AWS_DEFAULT_REGION environment variable (set by GitHub Actions)
        region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        bucket_suffix = f"{account_id}-{region}" if account_id and region else account_id or region or ''
        bucket_name = f"{base_bucket_name}-{bucket_suffix}" if bucket_suffix else base_bucket_name
        
        # Determine removal policy
        bucket_removal_policy = RemovalPolicy.DESTROY  # Change to RETAIN for production

        s3_client = boto3.client('s3')
        bucket_exists = False
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists. Importing.")
            bucket_exists = True
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Bucket '{bucket_name}' does not exist. Will be created.")
                bucket_exists = False
            else:
                # Re-raise other client errors
                raise e

        if bucket_exists:
            # If the bucket exists, import it
            bucket = s3.Bucket.from_bucket_name(
                self, "DataBucket",
                bucket_name=bucket_name
            )
        else:
            # If the bucket does not exist, create it
            bucket = s3.Bucket(
                self, "DataBucket",
                bucket_name=bucket_name,
                removal_policy=bucket_removal_policy,
                auto_delete_objects=params.get('auto_delete_objects', False),
                versioned=True
            )

        # Deploy config/parameters.json to S3 bucket
        s3deploy.BucketDeployment(self, "ConfigDeployment",
            sources=[s3deploy.Source.asset("config")],
            destination_bucket=bucket,
            destination_key_prefix="config/",
            include=["parameters.json"]
        )
