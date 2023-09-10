import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def publisg_msg(sns_client, topic_arn):
    """
    this function will send the email to users 
    if any error will occure while uploading file
    """
    resp = sns_client.publish(TopicArn = topic_arn,
            Subject="Lambda Function Error",
            Message="Your file could not be read and written to S3"
        )
    return resp
    
    
def get_json_data(s3_client, event):
    """
    this function reads the content of s3 object from the rource location
    """
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        key_name = event['Records'][0]['s3']['object']['key']
        
        response = s3_client.get_object(Bucket = bucket_name, Key = key_name)
        final_resp = response['Body'].read().decode('utf-8')
        
        return {
            'statusCode': 200,
            'body': json.dumps(final_resp)
        }
    except Exception:
        raise 
    
    
def upload_file(s3_client, event):
    """
    this function reads the content of s3 object from the rource location 
    and then upload the s3 object to destination 
    """
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        key_name = event['Records'][0]['s3']['object']['key']
        
        response = s3_client.get_object(Bucket = bucket_name, Key = key_name)
        final_resp = response['Body'].read().decode('utf-8')
        
        new_key = 'processed/' + key_name.split('/')[-1]
        s3_client.put_object(Bucket = bucket_name, Key = new_key, Body = final_resp)
        
        return {
            'statusCode': 200,
            'body': json.dumps(final_resp)
        }
    except Exception:
        raise 
    

def delete_file(s3_client, event):
    """
    this function will delete the s3 object from the rource location
    """
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        key_name = event['Records'][0]['s3']['object']['key']
        
        response = s3_client.delete_object(Bucket = bucket_name, Key = key_name) 
        return {
                'statusCode': 200,
                'body': json.dumps("Json File is delete from rourse folder")
            }
    except Exception:
        raise 