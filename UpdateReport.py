import json
import urllib.parse
import boto3
import os
import datetime
print('Loading function')

s3 = boto3.client('s3')
# buckts must in the same region as event bucket
text_bucket = 'textracttext'
report_bucket = 'textractreport'
image_bucket = 'textractimage'
pdf_bucket = 'textractpdf1321'

report_key = 'textract/report/usage.json'

image_folder = 'textract/image/'
text_folder = 'textract/text/'
pdf_folder = 'textract/document/'
summary_folder = 'textract/summary/'


def lambda_handler(event, context):
    #Extract info from event
    summary_bucket = event['Records'][0]['s3']['bucket']['name']
    summary_key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    summary_region = event['Records'][0]['awsRegion']
    summary_event_time = event['Records'][0]['eventTime']
    filename = os.path.split(summary_key)[1]

    print('summary_key 111 ' + summary_key)

    # print('image_bucket ' + image_bucket)
    image_name = ''
    image_key = get_key_from_bucket(image_bucket, filename)
    if image_key is None:
        #it's multiple
        image_key = get_multiple_from_image(image_bucket, filename)
        image_name = os.path.split(image_key)[0]

    else:
        image_name = os.path.split(image_key)[1]

    print('file name ' + filename)
    
    
    try:
        #Make sure image_content exist
        print('image_key ' + image_key)
        print('image_name ' + image_name)
        image_content = s3.get_object(Bucket=image_bucket, Key=image_key)

    except Exception as e:
        print(e)
        print('Error getting object image from bucket image. Make sure they ')
        raise e

    try:
        print('summary_bucket ' + summary_bucket)
        print('summary_key ' + summary_key)
        summary_content = s3.get_object(Bucket=summary_bucket, Key=summary_key)
        
    except Exception as e:
        print(e)
        print('Error getting object summary from bucket summary.')
        raise e

    try:
        report_content = s3.get_object(Bucket=report_bucket, Key=report_key)
    except Exception as e:
        print(e)
        print('Error getting object report from bucket report.')
        raise e

    #Prepare info to create the paths
    text_key = text_folder + '/' + filename
    summary_last_modified = summary_content['LastModified']
    image_last_modified = image_content['LastModified']
    process_time = get_process_time(image_last_modified, summary_last_modified)
    body = report_content['Body'].read().decode('utf-8')

    summary_path = get_complete_path(summary_bucket, get_key_from_bucket(summary_bucket, filename))
    text_path = get_complete_path(text_bucket, get_key_from_bucket(text_bucket, filename))



    #insert new data to the report
    json_array = []
    if len(body) is 0:
        item = {
            'date': str(summary_last_modified.date()),
            'image_name': image_name,
            'text_path': text_path,
            'summary_path': summary_path,
            'process_time': process_time.seconds
        }
    else:
        json_array = json.loads(body)
        item = {
            'date': str(summary_last_modified.date()),
            'image_name': image_name,
            'text_path': text_path,
            'summary_path': summary_path,
            'process_time': process_time.seconds
        }
    print(item)
    json_array.append(item)
    json_body = json.dumps(json_array)
    try:
        s3.put_object(Body=json_body, Bucket=report_bucket, Key=report_key)
    except Exception as e:
        print(e)
        print('Error updatting report')
        raise e
    return 'Done'

#get the middle path e.g. 2019/08/16/, excluding the folder and filename
#folder must have at least 3 levels
def get_middle_path(key):
    head = os.path.split(key)[0]
    headArr = head.split('/')
    middle = ''
    for i in range(2, len(headArr)):
        middle += headArr[i] + '/'
    return middle


#create the path using the given param
def get_complete_path(bucket, key):
    return os.path.join(bucket, key)

def get_process_time(start, end):
    return end - start

def get_key_from_bucket(bucket, filename):
    filename_partial = os.path.splitext(filename)[0]

    for key in s3.list_objects(Bucket=bucket)['Contents']:
        keyname = os.path.split(key['Key'])[1]
        keypath =  os.path.split(key['Key'])[0]
        keyname_partial = os.path.splitext(keyname)[0]
        if filename_partial == keyname_partial:
            # print(os.path.join(keypath, keyname))
            return os.path.join(keypath, keyname)
        
            
    return None


def get_multiple_from_image(image_bucket, filename):
    filename_partial = os.path.splitext(filename)[0]
    for key in s3.list_objects(Bucket=image_bucket)['Contents']:
        keyname = os.path.split(key['Key'])[1]
        keypath =  os.path.split(key['Key'])[0]
        folder = keypath.split('/')[-1]
        
        if folder == filename_partial:
            return os.path.join(keypath, keyname)
            print('multiple image path ' + os.path.join(keypath, keyname))
