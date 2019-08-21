# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 15:03:09 2019

@author: Valued Customer
"""

#import gensim
#import boto3
#from boto3 import client
#from gensim.summarization.summarizer import summarize
#import os
#import datetime
#from fpdf import FPDF

TARG=1000

import datetime
import os
import boto3
from boto3 import client

from gensim.summarization.summarizer import summarize
from fpdf import FPDF

def try_imports():
    try:
        import gensim
        from gensim.summarization.summarizer import summarize
        from fpdf import FPDF
        return 'Success'
    except Exception as e:
        print(str(e))
        return str(e)
    return 'No'

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
        

#Swag
def main(args):
    
    starttime=str(datetime.datetime.now())
    starttime=starttime.replace('.','_')
    starttime=starttime.replace(':','-')
    cwd=os.getcwd()
    
    #cwd=cwd.replace(' ','\ ')
    
    logfile=open(cwd+'\\log'+starttime+'.txt','w')
    logfile.write(cwd+'\n')
    logfile.write(starttime+'\n')
    
    
    
    
    s3 = boto3.client('s3',    aws_access_key_id=my_id,
        aws_secret_access_key=my_access_key)
    
    s32 = boto3.resource('s3',    aws_access_key_id=my_id,
                         aws_secret_access_key=my_access_key)
    
    result=try_imports()
    if result != 'Success':
        print('Imports Failed, Closing Early')
        logfile.write(result)
        logfile.close()
        s32.Bucket('sumtestlogs').upload_file(cwd+'\\log'+starttime+'.txt', cwd+'\\log'+starttime+'.txt' )
        return 0
    
    
    response = s3.list_buckets()
    #print(response)
    
    keylist=[]
    conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
    
    for key in conn.list_objects(Bucket='textracttext')['Contents']:
        if('.txt' in key['Key']):
            keylist.append(key['Key'])
    print('KEY LIST 1:')
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
                shortkey=key['Key']
                shortkey=shortkey.split('/summary/')[1]
                keylist2.append(shortkey)
    print('KEY LIST TWO')
    print(keylist2)
        
    
    logfile.write('# Textfiles in Input_Bucket:'+str(len(keylist))+'\n')
    logfile.write('# Textfiles in Output_Bucket:'+str(len(keylist2))+'\n')
    
    logfile.write('New files Summarized:'+'\n')             
    for key in keylist:
        print('key is',key)
        
        short_key=key.split('/text/')[1]
        short_key=short_key.replace('single/','')
        short_key=short_key.replace('multiple/','')
        
        if( short_key not in keylist2):
            print('summary_key', short_key)
            
            logfile.write(key+'\n') 
            obj = s32.Object('textracttext', key)
            text=obj.get()['Body'].read().decode('utf-8') 
            
            if len(text)<1:
                text='No Text Provided'
            default_ratio=.5
            
            if len(text)>1000:
                default_ratio=.25

            try:        
                summ=summarize(text,ratio=default_ratio)
                if(len(summ)==0):
                    summ=text+'\n [0 length summary generated]'
            except Exception as e:
                print(e)
                summ=text+'\n {Summary Error}'
                logfile.write(str(e)+'(too short to summarize)'+'\n')
            
            
            file=open(cwd+'/temp'+starttime+'.txt','w')
            
            summ=clean_summary(summ)
            file.write(summ)
            file.close()
            
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font('Arial', 'B', 12)
            
            jump=10
            count=0
            for line in summ.split('\n'):
                print(line)
                pdf.cell(200,10,txt=line,ln=1)
                count+=1
            pdfname=key.replace('.txt','')
            pdfname=pdfname.split('/')
            pdfname=pdfname[len(pdfname)-1]
            pdf.output(cwd+'/'+pdfname+'.pdf', 'F')
            
            print('outputkey', key)
            
            output = key
            if 'textract/text/' not in output:
                output='textract/text/'+output
            
            output=output.replace('/text','/summary')
            output=output.replace('/single','')
            output=output.replace('/multiple','')
            
            
            
            
            s32.Bucket('textractsummary').upload_file(cwd+'/temp'+starttime+'.txt',
                      output)
            
            output2=output.replace('/summary','/summarypdf')
            output2=output2.replace('.txt','.pdf')
            
            s32.Bucket('textractsummary').upload_file(cwd+'/'+pdfname+'.pdf',
                      output2)
            
            os.remove(cwd+'/temp'+starttime+'.txt')
    
    logfile.close()
    s32.Bucket('sumtestlogs').upload_file(cwd+'\\log'+starttime+'.txt', cwd+'\\log'+starttime+'.txt' )
    
main(0)
   
