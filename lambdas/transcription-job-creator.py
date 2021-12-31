from __future__ import print_function
import time
import boto3
import json

acceptable_file_extensions = ['mp4','wav', 'mp3']
def lambda_handler(event, context):
    # TODO implement
    print(event)
    transcribe = boto3.client('transcribe')
    
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
    storage_bucket = "project-transcription-outputs" # Bucket to store transcription json objects to
    storage_key = "videos/" # Directory in storage bucket to store transcription objects
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat=file_extension,
        LanguageCode='en-US',
        OutputBucketName=storage_bucket,
        OutputKey=storage_key
    )
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(5)
    print(status)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Trancription job completed')
    }
