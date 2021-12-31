from __future__ import print_function

import boto3
import json
import decimal
import requests
import time
from datetime import datetime
from requests_aws4auth import AWS4Auth
from requests.auth import HTTPBasicAuth
from collections import defaultdict
import logging
import botocore.config

import os, sys

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    # TODO implement
    print(event)
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.debug('## EVENT')
    logger.debug(event)
    # TODO implement
    def type_converter(obj):
        if isinstance(obj, datetime):
            return obj.__str__()
    # print(time.ctime(), event.keys(), "\nEVENT:\n", event)
    # print("-"*50)
    query = event['queryStringParameters']['q']
    print(query)
    logger.debug(query)
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
    
    myOpenSearchUrl = "https://search-photos-no-vpc-wnawrz54b6kh4bqyav7idotsdu.us-east-1.es.amazonaws.com/"
    myOpenSearchUrl += 'project_video/_doc'
    
    query = event['queryStringParameters']['q']
    
    keywords = query.split(' ')
    keywords_final = [k.replace(',','') for k in keywords]
    
    URL_end = ''
    for w in keywords:
        URL_end += 'q=labels:{}&'.format(w)
    URL_end = URL_end[:-1]
    
    searchURL = myOpenSearchUrl + "/_search?" + URL_end
    search_response = requests.get(searchURL, auth=auth, headers=headers).json()
    logger.debug(search_response)
    
    videos = defaultdict()
    
    for s in search_response['hits']['hits']:
        if s['_source']['objectKey'] not in videos:
            videos[s['_source']['objectKey']]=s['_source']['labels']
    
    result = {"results":list()}
    
    bucketname = 'project-temp-bucket'
    for im, lb in videos.items():
        url = "https://{}.s3.amazonaws.com/{}".format(bucketname, im)        
        labels = ''
        # for l in lb:
        #     labels += l + ', '
        result["results"].append({"url":url, "labels":[lb]})
    
    logger.debug(result)
    print('\n', result, '\n')
    
    return {
        'statusCode': 200,
        'headers': {
        'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(result)
    }