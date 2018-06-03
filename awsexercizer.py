  # Copyright 2018 BigBitBus Inc.

  # Licensed under the Apache License, Version 2.0 (the "License");
  # you may not use this file except in compliance with the License.
  # You may obtain a copy of the License at

  #     http://www.apache.org/licenses/LICENSE-2.0

  # Unless required by applicable law or agreed to in writing, software
  # distributed under the License is distributed on an "AS IS" BASIS,
  # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  # See the License for the specific language governing permissions and
  # limitations under the License.

import os, sys, time
from os.path import join, getsize, exists
import boto3, botocore
from botocore.client import ClientError
from exercizer import Exercizer
from traceback import format_exc
from pprint import pprint
import pickle
class AWSExercizer(Exercizer):
    def __init__(self, env_credentials= {}, region_name ='us-east-1'):
        Exercizer.__init__(self)
        if region_name == 'us-east-1': # aws quirk - you can't specify us-east-1 explicitly
            self.storage_client = boto3.client('s3',
                aws_access_key_id = os.environ.get(env_credentials['account']), 
                aws_secret_access_key = os.environ.get(env_credentials['secret']))
        else:
            self.storage_client = boto3.client('s3',
                aws_access_key_id = os.environ.get(env_credentials['account']), 
                aws_secret_access_key = os.environ.get(env_credentials['secret']),
                region_name=region_name)
        self.region_name = region_name
            

    def UploadObjectsToContainer(self, container_name='blobtester', localDir = '/tmp/smalldir'):
        # create bucket if it does not exist
        try:
            self.storage_client.head_bucket(Bucket=container_name)
        except ClientError: 
            if self.region_name != 'us-east-1':
                container = self.storage_client.create_bucket(
                    Bucket=container_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    })
            else:
                 container = self.storage_client.create_bucket(Bucket=container_name)

        dic_uploadData = {}
        for root, dirs, files in os.walk(localDir):
            for name in files:
                filePath = join(root,name)
                self.startTimer()
                try:
                    self.storage_client.upload_file(filePath, container_name, filePath)
                    dic_uploadData[filePath] = (self.endTimer(), getsize(filePath))
                except:
                    print ('Failure uploading {}'.format(filePath))
                    print (format_exc())
                    self.endTimer()
                    dic_uploadData[filePath] = -1   
        return dic_uploadData

    def ListObjectsInContainer(self, container_name = 'blobtester'):
        '''
        Return generator with the list of blobs
        '''
        return self.storage_client.list_objects_v2(Bucket=container_name)['Contents']
        
    def DownloadObjectsFromContainer(self, container_name = 'blobtester', localDir = '/tmp/smalldir'):
        if not exists(localDir):
            os.makedirs(localDir)
        dic_downloadData = {}
        self.startTimer()
        blobList = self.ListObjectsInContainer(container_name)
        dic_downloadData['_listObjectsInContainer'] = (self.endTimer(), "Container objects listing time") 
        for aBlob in blobList:
            self.startTimer()
            localPath = join(localDir,aBlob['Key'].split('/'))
            self.storage_client.download_file(container_name, aBlob['Key'], localPath)
            dic_downloadData[localPath] = (self.endTimer(), getsize(localPath))
        return dic_downloadData

    def DeleteContainer(self, container_name='blobtester'):
        self.startTimer()
        blobList = self.ListObjectsInContainer(container_name)
        deleteList = []
        for aBlob in blobList:
            deleteList.append({'Key': aBlob['Key']})
        self.storage_client.delete_objects(Bucket = container_name, Delete = { 'Objects': deleteList })
        self.storage_client.delete_bucket(Bucket = container_name)
        return {container_name: self.endTimer(), 'operation':'Deleted'}
    

if __name__=="__main__":
    # These are names of the environmental variables (not the actual values)
    env_credentials = { 
        'account': 'S3KEY', 
        'secret':'S3SECRET'
    }
    awsex = AWSExercizer(env_credentials = env_credentials)
    # Upload
 #   print "Upload objects"
 #   pprint(awsex.UploadObjectsToContainer())
    # Download
 #   print "Download objects"
 #   pprint(awsex.DownloadObjectsFromContainer())
    # Delete
  
    pickle.dump(awsex.UploadObjectsToContainer(localDir = sys.argv[1]), open('/tmp/outputdata/objbench/aws_upload.pkl','wb'))
    # Download
    pickle.dump(awsex.DownloadObjectsFromContainer(localDir = sys.argv[1]), open('/tmp/outputdata/objbench/aws_download.pkl','wb'))
    print "Delete buckets"
    pprint(awsex.DeleteContainer())