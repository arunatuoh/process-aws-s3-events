import json
import logging

from utils import publisg_msg, get_json_data, upload_file, delete_file
from constant import bucket_name, topic_arn
from clients import s3_client, sns_client

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # calling get_json_data function to get the s3 object 
        json_data = get_json_data(s3_client, event)
        if json_data.get('statusCode') == 200:

            # after getting successful response of get_json_data, calling update_file method to store s3 object in different location in same bucket
            upload_file(s3_client, event)
            
            # then deleting s3 object from main directory(from where we upload first time)
            delete_file(s3_client, event)
            
            resp =  200, json.dumps('JSON file written to a different folder in S3')
        
    except Exception as e:
        
        logger.error("An error occurred: " + str(e))
        resp_ps = publisg_msg(sns_client, topic_arn)
        resp = 500, json.dumps('Something went wrong')
        
    return dict.fromkeys(('statusCode', 'body'), resp)
