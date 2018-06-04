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
import google.cloud.storage
import pickle
import uuid
from pprint import pprint
from exercizer import Exercizer
from traceback import format_exc
class GCPExercizer(Exercizer):
    def __init__(self, 
        region_name = 'us-east1', 
        storage_class = 'REGIONAL', 
        fileSizeskb = [],
        localDir= '/tmp/localDir',
        container_name='blobtester',
        numIters = 1):
        Exercizer.__init__(self)
        self.storage_client = google.cloud.storage.Client()
        self.region_name = region_name
        self.storage_class = storage_class
        self.fileSizeskb = fileSizeskb
        self.localDir = localDir
        self.container_name =container_name
        self.numIters = int(numIters)

    def UploadObjectsToContainer(self):
        try:
            container = self.storage_client.bucket(self.container_name)
            container.location = self.region_name
            container.storage_class = self.storage_class
            container.create()
        except google.cloud.exceptions.Conflict:
            print "Bucket exists, continuing"
            container = self.storage_client.get_bucket(self.container_name)

        list_uploadData = []
        for ii in range(0, self.numIters):
            for fSizekb in self.fileSizeskb:
                intkb = int(fSizekb)
                filePath = join(self.localDir,str(uuid.uuid4()))
                self.makeOneRandomBinFile(filePath, intkb)
                blob = container.blob(filePath)
                self.startTimer()
                try:
                    blob.upload_from_filename(filePath)
                    list_uploadData.append((self.endTimer(), getsize(filePath)))
                except:
                    print ('Failure uploading {}'.format(filePath))
                    print (format_exc())
                    self.endTimer()
                    list_uploadData[filePath] = -1
                os.remove(filePath)   
        return list_uploadData

    def ListObjectsInContainer(self):
        '''
        Return generator with the list of blobs
        '''
        bucket = self.storage_client.get_bucket(self.container_name)
        return bucket.list_blobs()
        
    def DownloadObjectsFromContainer(self):
        if not exists(self.localDir):
            os.makedirs(self.localDir)
        list_downloadData = []
        blobListGenerator = self.ListObjectsInContainer()
        #dic_downloadData['_listObjectsInContainer'] = (self.endTimer(), "Container objects listing time") 
        for aBlob in blobListGenerator:
            localPath = join(self.localDir,aBlob.name.split('/')[-1])
            self.startTimer()
            aBlob.download_to_filename(localPath)
            list_downloadData.append((self.endTimer(), getsize(localPath)))
            os.remove(localPath)
            aBlob.delete() # Immediate deletion
        return list_downloadData

    def DeleteContainer(self):
        self.startTimer()
        blobList = self.ListObjectsInContainer()
        for aBlob in blobList:
            aBlob.delete()
        bucket = self.storage_client.get_bucket(self.container_name)
        bucket.delete()
        return {self.container_name: self.endTimer(), 'operation':'Deleted'}
    


if __name__=="__main__":
    # For GCE, there are no credentials to read in. The sdk driver itself
    # uses the json credentials file pointed to by the 
    # GOOGLE_APPLICATION_CREDENTIALS OS environment variable
    gcpex = GCPExercizer(localDir = sys.argv[1], numIters = sys.argv[2], fileSizeskb = sys.argv[3:])
    # Upload
    pickle.dump(gcpex.UploadObjectsToContainer(),open('/tmp/outputdata/objbench/gcp_upload.pkl','wb'))
    # Download
    pickle.dump(gcpex.DownloadObjectsFromContainer(),open('/tmp/outputdata/objbench/gcp_download.pkl','wb'))
    # Delete
    pprint(gcpex.DeleteContainer())
