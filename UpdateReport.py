import json
import urllib.parse
import boto3
import os
import datetime
print('Loading function')

s3 = boto3.client('s3')
report_key = 'matt/report/usage.json'
report_key_gz = 'matt/reportgz/usage.json.gz'
image_folder = 'imgstore/'
text_folder = 'imgstore/'
pdf_folder = '/'

# buckts must in the same region as event bucket
text_bucket = 'textracttext'
report_bucket = 'textractreport'
image_bucket = 'textractimage'
pdf_bucket = 'textractpdf1321'


def lambda_handler(event, context):
    summary_bucket = event['Records'][0]['s3']['bucket']['name']
    summary_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    summary_region = event['Records'][0]['awsRegion']
    summary_event_time = event['Records'][0]['eventTime']

    filename = os.path.split(summary_key)[1]
    # print(filename)

    image_name = get_image_filename(image_bucket, image_folder, filename)
    
    text_key = text_folder + '/' + filename
    text_path = get_path(text_bucket, text_key, summary_region)
    summary_path = get_path(summary_bucket, summary_key, summary_region)

    try:
        summary_content = s3.get_object(Bucket=summary_bucket, Key=summary_key)
        # print(summary_content)
        image_content = s3.get_object(
            Bucket=image_bucket, Key=image_folder + image_name)
        # print(image_content)
        report_content = s3.get_object(Bucket=report_bucket, Key=report_key)
        # print(report_content)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they '
              + 'exist and your bucket is in the same region as this function.')
        raise e


        
    summary_last_modified = summary_content['LastModified']
    # print(summary_last_modified)
    image_last_modified = image_content['LastModified']
    # print(image_last_modified)
    process_time = get_process_time(
        image_last_modified, summary_last_modified)
    # print(process_time.seconds)
    body = report_content['Body'].read().decode('utf-8')

    json_array = []
    if len(body) is 0:
        # print('len(body) is 0')
        item = {
            'id': 1,
            'image_name': image_name,
            'text_path': text_path,
            'summary_path': summary_path,
            'process_time': process_time.seconds
        }
        json_array.append(item)

    else:
        # print('len(body) > 0')
        json_array = json.loads(body)
        item = {
            'id': int(json_array[-1]['id']) + 1,
            'image_name': image_name,
            'text_path': text_path,
            'summary_path': summary_path,
            'process_time': process_time.seconds
        }
        json_array.append(item)
 
    json_body = json.dumps(json_array)
    s3.put_object(Body=json_body, Bucket=report_bucket, Key=report_key)
    # print(json_body)

    return 'Done'


def get_process_time(start, end):
    return end - start


def get_path(bucket, key, region):
    return 'https://' + bucket + '.s3-' + region + '.amazonaws.com/' + key

def get_pdf_filename(bucket, pdf_folder, filename):
    s3res = boto3.resource('s3')
    pdf_folder = s3res.Bucket(bucket)
    filename_partial = os.path.splitext(filename)[0]
    pdf = pdf_folder + filename_partial + '.pdf'

    pdfObj = list(image_bucket.objects.filter(Prefix=pdf))
    if len(pdfObj) and pdfObj[0].key == pdf:
        return os.path.split(pdf)[1]
    return None


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
