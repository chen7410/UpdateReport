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

    summary_content = s3.get_object(Bucket=summary_bucket, Key=summary_key)
    summary_last_modified = summary_content['LastModified']

    print('summary_key ' + summary_key)

    # print('image_bucket ' + image_bucket)
    
    image_key = get_last_modified_key_from_bucket(image_bucket, filename, summary_last_modified)
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
        report_content = s3.get_object(Bucket=report_bucket, Key=report_key)
    except Exception as e:
        print(e)
        print('Error getting object report from bucket report.')
        raise e

    #Prepare info to create the paths
    text_key = text_folder + '/' + filename
    
    image_last_modified = image_content['LastModified']
    process_time = get_process_time(image_last_modified, summary_last_modified)
    body = report_content['Body'].read().decode('utf-8')

    summary_path = get_complete_path(summary_bucket, get_last_modified_key_from_bucket(summary_bucket, filename, summary_last_modified))
    text_path = get_complete_path(text_bucket, get_last_modified_key_from_bucket(text_bucket, filename, summary_last_modified))



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


#create the path using the given param
def get_complete_path(bucket, key):
    return os.path.join(bucket, key)

def get_process_time(start, end):
    # print('start ' + str(start))
    # print('end ' + str(end))
    return end - start

def get_last_modified_key_from_bucket(bucket, filename, last_modified):
    filename_partial = os.path.splitext(filename)[0]
    key_match = {}
    #find all match keys
    for key in s3.list_objects(Bucket=bucket)['Contents']:
        # print('key ' + str(key))
        keyname = os.path.split(key['Key'])[1]
        keypath =  os.path.split(key['Key'])[0]
        keyname_partial = os.path.splitext(keyname)[0]
        if filename_partial == keyname_partial:
            key_match[key['LastModified']] = os.path.join(keypath, keyname)
    
    #find the least key from the match key
    least_key = ''
    big_int = 99999
    second = datetime.timedelta(big_int)
    # print('last_modified ' + str(last_modified))
    for key in key_match:
        # print('key modified ' + str(key) + 'key path ' + key_match[key])
        if get_process_time(key, last_modified) < second:
            second = last_modified - key
            # print('second ' + str(second))
            least_key = key_match[key]
    # print('least_key return ' + str(least_key) + 'time lap ' + str(second))
    return least_key

