from __future__ import print_function
import time
import boto3
import json
import os, logging, sys
import botocore.config
import requests
from datetime import datetime
from requests_aws4auth import AWS4Auth
from text_processing import is_Stopword
import functools
import itertools
from functools import reduce
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    # TODO implement
    a = 'i'
    print(is_Stopword(a))
    logger.debug(event)
    bucketname = event['Records'][-1]['s3']['bucket']['name']
    origin_filename = event['Records'][-1]['s3']['object']['key']
    logger.debug(bucketname)
    logger.debug(origin_filename)
    
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
    file = origin_filename.split('.')[0] # File name
    file_extension = origin_filename.split('.')[-1] # File type, typically mp4
    if file_extension not in acceptable_file_extensions:
        raise Exception("{} is not an acceptable file type!".format(file_extension))
    else:
        print("{} is an acceptable file type!".format(file_extension))
    s3 = boto3.client('s3')
    comp = boto3.client('comprehend')
    compmed = boto3.client('comprehendmedical')
    
    response = s3.get_object(Bucket=bucketname, Key=origin_filename)
    transcribe_response = response['Body'].read().decode('utf-8')
    transcribe_json = json.loads(transcribe_response)
    print(len(transcribe_response), type(transcribe_response))
    print("Transcribe_json- ", transcribe_json.__len__(), type(transcribe_json))
    transcript = transcribe_json['results']['transcripts'][0]['transcript']
    
    if len(transcript) > 4900:
        transcript_mod = transcript[:4900]
        print("File is too big, needed to reduce it.")
    else:
        transcript_mod = transcript
    
    print("Transcript length = ", len(transcript_mod))
    key_phrases_job = comp.detect_key_phrases(Text=transcript_mod, LanguageCode='en')
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
    med_job = compmed.detect_entities_v2(Text=transcript_mod)
    med_ent = med_job['Entities']
    if len(med_ent) != 0:
        for m in med_ent:
            if m['Category'] != 'PROTECTED_HEALTH_INFORMATION':
                med_list.append(m['Text'])
        med_list = list(set(med_list))
    
    types = ['PERSON','LOCATION','ORGANIZATION','COMMERCIAL_ITEM','EVENT','TITLE','OTHER']
    types = set(types)
    
    entity_job = comp.detect_entities(Text=transcript_mod, LanguageCode='en')
    entities = entity_job['Entities']
    entity_list = list()
    
    if len(entities)>0:
        for e in entities:
            if e['Type'] in types:
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
    
    file_bucket = 'project-temp-bucket'
    
    print("File prefix = ", file)
    logger.debug(file)
    logger.debug(combined_list)
    
    file_prefix = file.split('/')[-1][:-1]
    print("\n", file_prefix, "\n")
    response = s3.list_objects(Bucket=file_bucket, Prefix=file_prefix)
    print("\n----------\n")
    print(response)
    print("\n----------\n")
    if len(response['Contents']) == 1:
        uri_file = response['Contents'][0]['Key']
    else:
        raise Exception("No valid file found!")
    
    print('BEFORE INDEX LABELS:\n',combined_list, '\n')
    
    if len(combined_list) < 1:
        raise Exception("NO VALID KEYWORDS")
    else:
        final_labels = combined_list
    
    test_obj = {
        "objectKey": uri_file,
        "bucket": file_bucket,
        "createdTimestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "labels": final_labels
    }
    
    myOpenSearchUrl = "https://search-photos-no-vpc-wnawrz54b6kh4bqyav7idotsdu.us-east-1.es.amazonaws.com/"

    myOpenSearchUrl += 'project_video/_doc'
    
    indexing_results = requests.post(myOpenSearchUrl, auth=auth, json=test_obj, headers=headers)
    
    indexing_results_str = str(indexing_results)
    
    if indexing_results_str != '<Response [201]>':
        raise Exception("INDEXING WAS UNSUCCESSFUL!")
    else:
        print("Indexing worked.")
        
    return {
        'statusCode': 200,
        'body': json.dumps(indexing_results_str)
    }
