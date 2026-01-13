import json
import boto3
import os

class StorageClient:
    def __init__(self, endpoint_url="http://localhost:9000", 
                 aws_access_key_id="minioadmin", 
                 aws_secret_access_key="minioadmin",
                 bucket_name="datalake"):
        
        # In a real app, prefer env vars or IAM roles
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket = bucket_name

    def get_user_factors(self):
        """
        Reads {id: [vector]} from S3
        """
        return self._read_folder_as_dict("models/user_factors_json", "id", "features")

    def get_recommendations(self):
        """
        Reads {userId: [{movieId: 1, rating: 5.0}, ...]}
        """
        return self._read_folder_as_dict("models/user_recs_json", "userId", "recommendations")

    def _read_folder_as_dict(self, prefix, key_field, val_field):
        data = {}
        try:
            # S3 doesn't have directories, so we list by prefix
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

            for page in pages:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    key = obj['Key']
                    # Spark parts usually start with part- and end with .json for JSON output
                    # Check if it looks like a data file (not _SUCCESS)
                    if "part-" in key and key.endswith(".json"):
                        response = self.s3.get_object(Bucket=self.bucket, Key=key)
                        content = response['Body'].read().decode('utf-8')
                        
                        # Spark writes Line-Delimited JSON
                        for line in content.splitlines():
                            if line.strip():
                                try:
                                    row = json.loads(line)
                                    data[row[key_field]] = row[val_field]
                                except json.JSONDecodeError:
                                    pass
            
            return data

        except Exception as e:
            print(f"S3 Read Error ({prefix}): {e}")
            return {}
