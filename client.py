import boto3


# creating here s3 client
s3_client = boto3.client('s3')

# creating here sns client
sns_client = boto3.client('sns')