from __future__ import print_function
import time
import boto3
import json
import os, logging, sys
import botocore.config
import requests
from datetime import datetime
from requests_aws4auth import AWS4Auth
import inflection
from text_processing import is_Stopword
import functools
import itertools
from functools import reduce

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    # TODO implement
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.debug('## EVENT')
    logger.debug(event)
    
    def type_converter(obj):
        if isinstance(obj, datetime):
            return obj.__str__()
    
    credentials = boto3.Session().get_credentials()
    
    auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        'us-east-1',
        'es',
        session_token=credentials.token)
        
    headers = {
        "Content-Type": "application/json"
    }
    
    
    s3 = boto3.client('s3')
    comp = boto3.client('comprehend')
    compmed = boto3.client('comprehendmedical')
    
    acceptable_file_extensions = ['json']
    full_file = event['Records'][0]['s3']['object']['key']
    print(full_file)    
    file = full_file.split('.')[0] # File name
    file_extension = full_file.split('.')[-1] # File type, typically mp4
    if file_extension not in acceptable_file_extensions:
        print("{} is not an acceptable file type!".format(file_extension))
    else:
        print("{} is an acceptable file type!".format(file_extension))
    job_time = int(time.time())
    # job_name = file + '-' + str(job_time) + "-job-name"
    job_name = file + '-'
    bucket = event['Records'][0]['s3']['bucket']['name'] # Bucket that calls this function
    job_uri = "s3://{}/{}".format(bucket,full_file)
    print("file 2: {}.{}".format(file, file_extension))
    print("job-name: {}".format(job_name))
    print("bucket: {}".format(bucket))
    print("job uri: {}".format(job_uri))
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=full_file)
    textract_response = response['Body'].read().decode('utf-8')
    print(len(textract_response), type(textract_response))
    textract_json = json.loads(textract_response)
    print("Textractjson- ", textract_json.__len__(), type(textract_json))
    contents = ''
    for t in textract_json:
        if 'Text' in t.keys():
            contents += t['Text'] + " "
    contentsBytes = bytes(contents, 'utf-8')
    
    print("CONTENT BYTES LENGTH = ", len(contentsBytes))
    storage_bucket = "textract-results-ccbdproj-txt" # Bucket to store transcription json objects to
    s3Res = boto3.resource('s3')
    object = s3Res.Object(storage_bucket, '{}.txt'.format(file))
    
    result = object.put(Body=contentsBytes)
    
    contents_ByteLen = len(contentsBytes)
    
    if contents_ByteLen > 4900:
        print("File is too big, need to reduce it!")
        content_OLD = contents
        contents = contents[:4900]
    
    print("Contents Length = ", len(contents))
    key_phrases_list = list()
    
    key_phrases_job = comp.detect_key_phrases(Text=contents, LanguageCode='en')
    key_phrases = key_phrases_job['KeyPhrases']
    big_final_list = list()
    
    for k in key_phrases:
        temp1 = k['Text'].split(' ')
        for t in temp1:
            spt = t.split("'")
            if len(spt)==2:
                if is_Stopword(spt[0]) or is_Stopword(spt[0]):
                    pass
            elif len(spt)==1:
                if is_Stopword(spt[0]):
                    pass
                else:
                    big_final_list.append(spt[0])
    big_final_list = list(set(big_final_list))
    
    med_list = list()
    med_job = compmed.detect_entities_v2(Text=contents)
    med_ent = med_job['Entities']
    if len(med_ent) != 0:
        for m in med_ent:
            # if m['Category'] != 'PROTECTED_HEALTH_INFORMATION':
            med_list.append(m['Text'])
        med_list = list(set(med_list))
    
    types = ['PERSON','LOCATION','ORGANIZATION','COMMERCIAL_ITEM','EVENT','TITLE','OTHER']
    types = set(types)
        
    entity_job = comp.detect_entities(Text=contents, LanguageCode='en')
    entities = entity_job['Entities']
    entity_list = list()
    
    if len(entities)>0:
        for e in entities:
            # if e['Type'] in types:
            entity_list.append(e['Text'])
    entity_list = list(set(entity_list))
    
    if len(entities)>0:
        for e in entities:
            # if e['Type'] in types:
            entity_list.append(e['Text'])
    entity_list = list(set(entity_list))
    
    if len(med_list) != 0 and len(entity_list) != 0:
        combined_list = list(itertools.chain(big_final_list, med_list, entity_list))
            
    elif len(med_list) != 0 and len(entity_list) == 0:
        combined_list = list(itertools.chain(big_final_list, med_list))
        
    elif len(med_list) == 0 and len(entity_list) != 0:
        combined_list = list(itertools.chain(big_final_list, entity_list))
        
    else:
        combined_list = big_final_list
    
    combined_list = list(set(combined_list))
    
    file_bucket = 'temp-bucket-testing-23'
    
    print("File prefix = ", file)
    logger.debug(file)
    # logger.debug(combined_list)
    response = s3.list_objects(Bucket=file_bucket, Prefix=file)
    
    if len(response['Contents']) >= 1:
        uri_file = response['Contents'][0]['Key']
    else:
        raise Exception("No valid file found!")
    
    print('BEFORE INDEX LABELS:\n',combined_list, '\n')
    
    if len(combined_list) < 1:
        raise Exception("NO VALID KEYWORDS")
    else:
        final_labels = combined_list
    
    headers = {
        "Content-Type": "application/json"
    }
    
    test_obj = {
        "objectKey": uri_file,
        "bucket": file_bucket,
        "createdTimestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "labels": final_labels
    }
    
    myOpenSearchUrl = "https://search-photos-no-vpc-wnawrz54b6kh4bqyav7idotsdu.us-east-1.es.amazonaws.com/"

    myOpenSearchUrl += 'project_images/_doc'
    
    indexing_results = requests.post(myOpenSearchUrl, auth=auth, json=test_obj, headers=headers)
    
    indexing_results_str = str(indexing_results)
    
    if indexing_results_str != '<Response [201]>':
        raise Exception("INDEXING WAS UNSUCCESSFUL!")
    else:
        print("SUCCESS!!!")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps("PLACEHOLDER") # ('Trancription job completed')
    }
