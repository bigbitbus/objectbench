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

import os, sys, time, pickle
from os.path import join, getsize, exists
from azure.storage.blob import BlockBlobService
from pprint import pprint
from exercizer import Exercizer
from traceback import format_exc
class AzureExercizer(Exercizer): #No easy way of getting auto regions
    def __init__(
        self, 
        env_credentials,
        container_name='blobtester',
        fileSizeskb = [],
        localDir= '/tmp/self.localDir',
        numIters = 1):
        Exercizer.__init__(
            self,
            fileSizeskb,
            localDir,
            numIters)

        self.storage_client = BlockBlobService(
            os.environ.get(env_credentials['account']), 
            os.environ.get(env_credentials['secret']))
        self.container_name = container_name

    def UploadObjectsToContainer(self):
        self.storage_client.create_container(self.container_name)
        list_uploadData = []
        for eachFile in self.manifest:
            filePath, intfilesize = eachFile
            print "U",
            print eachFile
            self.makeOneRandomBinFile(filePath, intfilesize)
            self.startTimer()
            try:
                self.storage_client.create_blob_from_path(
                    self.container_name, 
                    filePath.split('/')[-1], 
                    filePath)
                list_uploadData.append( 
                    (self.endTimer(), getsize(filePath), 'azure_upload'))
            except:
                print ('Failure uploading {}'.format(filePath))
                print format_exc()
                self.endTimer() 
        return list_uploadData

    def ListObjectsInContainer(self):
        '''
        Return list with the list of blob names
        '''
        return list(self.storage_client.list_blobs(self.container_name))
        
    def DownloadObjectsFromContainer(self):
        if not exists(self.localDir):
            os.makedirs(self.localDir)
        list_downloadData = []
        blobList = self.ListObjectsInContainer()
        for aBlob in blobList:
            self.startTimer()
            localPath = join(self.localDir,aBlob.name.split('/')[-1])
            self.storage_client.get_blob_to_path(
                self.container_name, 
                aBlob.name, 
                localPath )
            blobsize = getsize(localPath)
            list_downloadData.append( 
                (self.endTimer(), blobsize , 'azure_download'))
            print "D",
            print (localPath, blobsize)
            os.remove(localPath)
            self.startTimer()
            self.storage_client.delete_blob(
                self.container_name, aBlob.name)
            list_downloadData.append( 
                (self.endTimer(), blobsize , 'azure_delete'))    
        return list_downloadData

    def DeleteContainer(self, container_name='blobtester'):
        self.startTimer()
        self.storage_client.delete_container(self.container_name)
        return {self.container_name: self.endTimer(), 'operation':'Deleted'}
    


if __name__=="__main__":
    # These are names of the environmental variables (not the actual values)
    env_credentials = { 
        #'account': 'AZBLOBACCOUNT', 
        #'secret':'AZBLOBKEY' #us east
        'account': 'AZBLOBACCOUNTCA', 
        'secret':'AZBLOBKEYCA' #Canada
    }

    azex = AzureExercizer(
        env_credentials,
        localDir = sys.argv[1], 
        numIters = sys.argv[2], 
        fileSizeskb = sys.argv[3:])
    # Upload
#    pprint(azex.UploadObjectsToContainer())
    # Download
#    pprint(azex.DownloadObjectsFromContainer())
    
    pickle.dump(
        azex.UploadObjectsToContainer(), 
        open('/tmp/outputdata/objbench/az_upload.pkl','wb'))
    # Download
    pickle.dump(
        azex.DownloadObjectsFromContainer(),
        open('/tmp/outputdata/objbench/az_download.pkl','wb'))

    # Delete
    print "Delete bucket"
    pprint(azex.DeleteContainer())
