# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 15:03:09 2019

@author: Valued Customer
"""

import gensim
import boto3
from boto3 import client
from gensim.summarization.summarizer import summarize
import os
import datetime
from fpdf import FPDF

TARG=1000

def clean_summary(summ):
    output=''
    prev=''
    doubleprev=''
    for line in summ.split('\n'):
        if line not in prev and line != doubleprev:
            output+=line+'\n'
        doubleprev=prev
        prev=line
    return output
        


def main(args):
    
    starttime=str(datetime.datetime.now())
    starttime=starttime.replace('.','_')
    starttime=starttime.replace(':','-')
    cwd=os.getcwd()
    
    #cwd=cwd.replace(' ','\ ')
    
    logfile=open(cwd+'\\log'+starttime+'.txt','w')
    logfile.write(cwd+'\n')
    logfile.write(starttime+'\n')
    
    s3 = boto3.client('s3',    aws_access_key_id='AKIA4F52A4XR6FM2QMP5',
        aws_secret_access_key='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')
    
    response = s3.list_buckets()
    #print(response)
    
    keylist=[]
    conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
    
    for key in conn.list_objects(Bucket='textracttext')['Contents']:
        if('.txt' in key['Key']):
            keylist.append(key['Key'])
    print(keylist)
    
    
    keylist2=[]
    g0=False
    try:
        a=conn.list_objects(Bucket='textractsummary')['Contents']
        g0=True
    except Exception as e:
        print(e)
    if g0:
        for key in conn.list_objects(Bucket='textractsummary')['Contents']:
            if('.txt' in key['Key']):
                keylist2.append(key['Key'])
    print('KEY LIST TWO')
    print(keylist2)
        
    
    s32 = boto3.resource('s3',    aws_access_key_id='AKIA4F52A4XR6FM2QMP5',
        aws_secret_access_key='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')
    
    logfile.write('# Textfiles in Input_Bucket:'+str(len(keylist))+'\n')
    logfile.write('# Textfiles in Output_Bucket:'+str(len(keylist2))+'\n')
    
    logfile.write('New files Summarized:'+'\n')             
    for key in keylist:
        print('key is',key)
        summary_key=key.replace('.txt','')+'.txt'
        
        if( summary_key not in keylist2):
            print('summary_key', summary_key)
            
            logfile.write(key+'\n') 
            obj = s32.Object('textracttext', key)
            text=obj.get()['Body'].read().decode('utf-8') 
            
            if len(text)<1:
                text='text too short'
            default_ratio=.1
            default_ratio=TARG/len(text)
            
            if default_ratio>.9:
                default_ratio=.9
            if default_ratio<.01:
                default_ratio=.01
            
            try:        
                summ=summarize(text,ratio=default_ratio)
                if(len(summ)==0):
                    summ=text
            except Exception as e:
                print(e)
                summ=text
                logfile.write(str(e)+'(too short to summarize)'+'\n')
            
            
            file=open(cwd+'/temp'+starttime+'.txt','w')
            
            summ=clean_summary(summ)
            file.write(summ)
            file.close()

            output = 'textract/summary' + key[13:]
            s32.Bucket('textractsummary').upload_file(cwd+'/temp'+starttime+'.txt',
                      output)
            
            os.remove(cwd+'/temp'+starttime+'.txt')
    
    logfile.close()
    s32.Bucket('sumtestlogs').upload_file(cwd+'\\log'+starttime+'.txt', cwd+'\\log'+starttime+'.txt' )
    
main(0)
   
