import json
import boto3

def save_to_s3(name, data):
    s3 = boto3.client('s3')
    bucket = 'hatt26-data-test'
    json_data = data
    fileName = name + '.json'
    uploadByteStream = bytes(json.dumps(json_data, ensure_ascii=False).encode('utf8'))
    s3.put_object(Bucket=bucket, Key=fileName, Body=uploadByteStream)