import json
import urllib.parse
import boto3
print('Loading function')

s3 = boto3.client('s3')
report_key = 'report/jsonfile.json'
report_key_gz = 'summary/jsonfile.json.gz'
report_bucket = 'quicksightdata753'
reggie_bucket_gz = 'summarytextract'
new_report_key = 'report/jsonfileAdded.json'

reggie_bucket = 'result.store'
reggie_key = 'testing.txt'


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(event)
    
    try:
        # reggie = s3.get_object(Bucket=reggie_bucket, Key=reggie_key)
        # print(reggie)
        content = s3.get_object(Bucket=report_bucket, Key=report_key)
        # print(content)
        body = content['Body'].read().decode('utf-8')
        json_array = json.loads(body)
        json_array.append({'id': 99, 'image': str(99)+'.png', 'textPath': str(99)+'.txt', 'textPathModfied': '2019-9-9 12:12:12',
        'summaryPath': str(99)+'summary.txt', 'summaryPathModfied': '2019-9-9 12:12:20', 'processTimeSecond': 99})
        # print(json_array[-1]['id'])
        
        # json_body = json.dumps(json_array)
        # s3.put_object(Body=json_body, Bucket=report_bucket, Key=new_report_key)
        
        

        return event
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
