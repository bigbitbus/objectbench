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
from os.path import join, getsize
from azure.storage.blob import BlockBlobService
from exercizer import Exercizer

class AzureExercizer(Exercizer):
    def __init__(self, env_credentials):
        Exercizer.__init__(self)
        self.storage_client = BlockBlobService(
            os.environ.get(env_credentials['account']), 
            os.environ.get(env_credentials['secret']))

    def UploadObjectsToContainer(self, container_name='blobtester', localDir = '/tmp/smalldir'):
        self.storage_client.create_container(container_name)
        dic_uploadData = {}
        for root, dirs, files in os.walk(localDir):
            for name in files:
                filePath = join(root,name)
                self.startTimer()
                try:
                    self.storage_client.create_blob_from_path(container_name, 
                        name, filePath)
                    dic_uploadData[filePath] = (self.endTimer(), getsize(filePath))
                except:
                    print ('Failure uploading {}'.format(filePath))
                    print (format_exc())
                    self.endTimer()
                    dic_uploadData[filePath] = -1   
        return dic_uploadData

    def ListObjectsInContainer(self, container_name = 'blobtester'):
        '''
        Return generator with the list of blob names
        '''
        return self.storage_client.list_blobs(container_name)
        
    def DownloadObjectsFromContainer(self, container_name = 'blobtester', localDir = '/tmp/smalldir'):
        dic_downloadData = {}
        self.startTimer()
        blobListGenerator = self.ListObjectsInContainer(container_name)
        dic_downloadData['_listObjectsInContainer'] = (self.endTimer(), "Container objects listing time") 
        for aBlob in blobListGenerator:
            self.startTimer()
            localPath = join(localDir,aBlob.name)
            self.storage_client.get_blob_to_path(container_name, aBlob.name, localPath )
            dic_downloadData[localPath] = (self.endTimer(), getsize(localPath))
        return dic_downloadData

    def DeleteContainer(self, container_name='blobtester'):
        self.startTimer()
        self.storage_client.delete_container(container_name)
        return {container_name: self.endTimer(), 'operation':'Deleted'}
    


if __name__=="__main__":
    # These are names of the environmental variables (not the actual values)
    env_credentials = { 
        'account': 'AZBLOBACCOUNT', 
        'secret':'AZBLOBKEY'
    }

    azex = AzureExercizer(env_credentials)
    # Upload
    print azex.UploadObjectsToContainer()
    # Download
    print azex.DownloadObjectsFromContainer()
    # Delete
    print azex.DeleteContainer()
