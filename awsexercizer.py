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

import os, sys, time, pickle, boto3, botocore
from os.path import join, getsize, exists
from botocore.client import ClientError
from traceback import format_exc
from pprint import pprint
from exercizer import Exercizer

class AWSExercizer(Exercizer):
    def __init__(
        self,
        env_credentials= {}, 
        region_name ='us-east-1',
        container_name='blobtester',
        fileSizeskb = [],
        localDir= '/tmp/localDir',
        numIters = 1):
        Exercizer.__init__(
            self,
            fileSizeskb,
            localDir,
            numIters)
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
        self.container_name = container_name
            

    def UploadObjectsToContainer(self):
        # create bucket if it does not exist
        try:
            self.storage_client.head_bucket(Bucket=self.container_name)
        except ClientError: 
            if self.region_name != 'us-east-1':
                container = self.storage_client.create_bucket(
                    Bucket=self.container_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    })
            else:
                 container = self.storage_client.create_bucket(
                     Bucket=self.container_name)

        list_uploadData = []
        for eachFile in self.manifest:
            filePath, intfilesize = eachFile
            print "U",
            print eachFile
            self.makeOneRandomBinFile(filePath, intfilesize)
            self.startTimer()
            try:
                self.storage_client.upload_file(
                    filePath, self.container_name, filePath)
                list_uploadData.append(
                    (self.endTimer(), getsize(filePath), 'aws_upload'))
            except:
                print ('Failure uploading {}'.format(filePath))
                print (format_exc())
                self.endTimer()   
            os.remove(filePath)  
        return list_uploadData

    def ListObjectsInContainer(self):
        '''
        Return generator with the list of blobs
        '''
        objList = self.storage_client.list_objects_v2(
            Bucket=self.container_name)
        if 'Contents' in objList:
            return objList['Contents']
        else:
            return []
        
    def DownloadObjectsFromContainer(self):
        if not exists(self.localDir):
            os.makedirs(self.localDir)
        list_downloadData = []
        blobList = self.ListObjectsInContainer()
        for aBlob in blobList:
            self.startTimer()
            localPath = join(self.localDir,aBlob['Key'].split('/')[-1])
            self.storage_client.download_file(
                self.container_name, 
                aBlob['Key'], localPath)
            blobsize = getsize(localPath)
            list_downloadData.append(
                (self.endTimer(), blobsize, 'aws_download'))
            print "D",
            print (localPath, blobsize)
            os.remove(localPath)
            self.startTimer()
            self.storage_client.delete_object(
                Bucket = self.container_name, 
                Key = aBlob['Key'])
            list_downloadData.append((self.endTimer(), blobsize, 'aws_delete'))
        return list_downloadData

    def DeleteContainer(self, container_name='blobtester'):
        self.startTimer()
        blobList = self.ListObjectsInContainer()
        deleteList = []
        for aBlob in blobList:
            deleteList.append({'Key': aBlob['Key']})
        if len(deleteList) > 0:
            self.storage_client.delete_objects(
                Bucket = self.container_name, 
                Delete = { 'Objects': deleteList })
        self.storage_client.delete_bucket(Bucket = self.container_name)
        return {self.container_name: self.endTimer(), 'operation':'Deleted'}
    

if __name__=="__main__":
    # These are names of the environmental variables (not the actual values)
    env_credentials = { 
        'account': 'S3KEY', 
        'secret':'S3SECRET'
    }
    awsex = AWSExercizer(
        env_credentials = env_credentials,
        localDir = sys.argv[1], 
        numIters = sys.argv[2], 
        fileSizeskb = sys.argv[3:],
        region_name ='ca-central-1') # us-east-1
  
    pickle.dump(
        awsex.UploadObjectsToContainer(),
            open('/tmp/outputdata/objbench/aws_upload.pkl','wb'))
    # Download
    pickle.dump(
        awsex.DownloadObjectsFromContainer(), 
        open('/tmp/outputdata/objbench/aws_download.pkl','wb'))
    print "Delete bucket"
    pprint(awsex.DeleteContainer())