import json
import urllib.parse
import boto3
import os
import datetime
print('Loading function')

s3 = boto3.client('s3')
report_key = 'matt/report/usage.json'
report_key_gz = 'matt/reportgz/usage.json.gz'
image_folder ='imgstore/'
text_folder = 'imgstore/'

#buckts must in the same region
text_bucket = 'textracttext'
report_bucket = 'textractreport'
image_bucket = 'textractimage'


def lambda_handler(event, context):
    summary_bucket = event['Records'][0]['s3']['bucket']['name']
    summary_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    summary_region = event['Records'][0]['awsRegion']
    summary_event_time = event['Records'][0]['eventTime']

    filename = os.path.split(summary_key)[1]
    # print(filename)
    suffix = filename.split('.')[-1]
    # print(suffix)
    
    try:
        image_name = get_image_filename(image_bucket, image_folder, filename)
        text_key = text_folder + '/' + filename
        text_path = get_text_path(text_bucket, text_key)
        summary_path = get_summary_path(summary_bucket, summary_key, summary_region)

        summary_content = s3.get_object(Bucket=summary_bucket, Key=summary_key)
        # print(summary_content)
        summary_last_modified = summary_content['LastModified']
        print(summary_last_modified)

        image_content = s3.get_object(Bucket=image_bucket, Key=image_folder + image_name)
        # print(image_content)
        image_last_modified = image_content['LastModified']
        print(image_last_modified)

        process_time = get_process_time(image_last_modified, summary_last_modified)
        print(process_time.seconds)
        


        report_content = s3.get_object(Bucket=report_bucket, Key=report_key)
        # print(report_content)
        body = report_content['Body'].read().decode('utf-8')
        # print(len(body))
        if len(body) is 0:
            print('len(body) is 0')
            item = {
                'id': 1,
                'image_name': image_name,
                'text_path': text_path,
                'summary_path': summary_path,
                'process_time': process_time.seconds
            }
        else:
            print('len(body) > 0')
        
        # json_array = json.loads(body)
        # if len(json_array)== 0:
        #     print('len is 0')
        # else:
        #     print('len > 0')
        
        # print(json_array[-1]['id'])
        
        # json_body = json.dumps(json_array)
        # s3.put_object(Body=json_body, Bucket=report_bucket, Key=new_report_key)
        
        return event
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they '
        + 'exist and your bucket is in the same region as this function.'
        .format(report_key, report_bucket))
        raise e

def get_process_time(start, end):
    return end - start

def get_text_path(bucket, key):
        return 's3://' + bucket + '/' + key
    
def get_summary_path(bucket, key, region):
        return 'https://' + bucket + '.s3-' + region + '.amazonaws.com/' + key

def get_image_filename(bucket, image_folder, filename):
    s3res = boto3.resource('s3')
    image_bucket = s3res.Bucket(bucket)
    filename_partial = os.path.splitext(filename)[0]
    png = image_folder + filename_partial + '.png'
    jpeg = image_folder + filename_partial + '.jpeg'
    pdf = image_folder + filename_partial + '.pdf'

    pngObj = list(image_bucket.objects.filter(Prefix=png))
    if len(pngObj) and pngObj[0].key == png:
        return os.path.split(png)[1]

    jpegObj = list(image_bucket.objects.filter(Prefix=jpeg))
    if len(jpegObj) and jpegObj[0].key == jpeg:
        return os.path.split(jpeg)[1]

    pdfObj = list(image_bucket.objects.filter(Prefix=pdf))
    if len(pdfObj) and pdfObj[0].key == pdf:
        return os.path.split(pdf)[1]
    return None

        # print(summary_last_modified)
        # {'ResponseMetadata': {'RequestId': '77AD6C7096E3BDC6', 
        # 'HostId': '/E6xFyL8QsEZ8nuki56q1Ar0uNXP3EG4B8ELqHtSYVsvnM7q6dWZfz9bDM+Ites2h1MUj+mMQMI=', 
        # 'HTTPStatusCode': 200, 
        # 'HTTPHeaders': {'x-amz-id-2': '/E6xFyL8QsEZ8nuki56q1Ar0uNXP3EG4B8ELqHtSYVsvnM7q6dWZfz9bDM+Ites2h1MUj+mMQMI=', 
        # 'x-amz-request-id': '77AD6C7096E3BDC6', 
        # 'date': 'Wed, 14 Aug 2019 06:20:17 GMT', 
        # 'last-modified': 'Tue, 13 Aug 2019 23:35:50 GMT', 
        # 'etag': '"2f6a56a24959bf995709f16c21bb506f"', 
        # 'accept-ranges': 'bytes', 
        # 'content-type': 'image/png', 
        # 'content-length': '109894', 
        # 'server': 'AmazonS3'}, 
        # 'RetryAttempts': 0}, 
        # 'AcceptRanges': 'bytes', 
        # 'LastModified': datetime.datetime(2019, 8, 13, 23, 35, 50, tzinfo=tzutc()), 
        # 'ContentLength': 109894, 
        # 'ETag': '"2f6a56a24959bf995709f16c21bb506f"', 
        # 'ContentType': 'image/png', 
        # 'Metadata': {}, 
        # 'Body': <botocore.response.StreamingBody object at 0x7f72f04ac080>}