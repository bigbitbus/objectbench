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
from pprint import pprint
from exercizer import Exercizer
from traceback import format_exc
class GCPExercizer(Exercizer):
    def __init__(self, region_name = 'us-east1', storage_class = 'REGIONAL'):
        Exercizer.__init__(self)
        self.storage_client = google.cloud.storage.Client()
        self.region_name = region_name
        self.storage_class = storage_class

    def UploadObjectsToContainer(self, container_name='blobtester', localDir = '/tmp/smalldir'):
        print "Using localDir {}".format(localDir)
        try:
            container = self.storage_client.bucket(container_name)
            container.location = self.region_name
            container.storage_class = self.storage_class
            container.create()
        except google.cloud.exceptions.Conflict:
            print "Bucket exists, continuing"
            container = self.storage_client.get_bucket(container_name)

        dic_uploadData = {}
        for root, dirs, files in os.walk(localDir):
            for name in files:
                filePath = join(root,name)
                blob = container.blob(filePath)
                self.startTimer()
                try:
                    blob.upload_from_filename(filePath)
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
        bucket = self.storage_client.get_bucket(container_name)
        return bucket.list_blobs()
        
    def DownloadObjectsFromContainer(self, container_name = 'blobtester', localDir = '/tmp/smalldir'):
        print "Using localDir {}".format(localDir)
        if not exists(localDir):
            os.makedirs(localDir)
        dic_downloadData = {}
        self.startTimer()
        blobListGenerator = self.ListObjectsInContainer(container_name)
        dic_downloadData['_listObjectsInContainer'] = (self.endTimer(), "Container objects listing time") 
        for aBlob in blobListGenerator:
            self.startTimer()
            localPath = join(localDir,aBlob.name)
            aBlob.download_to_filename(localPath)
            dic_downloadData[localPath] = (self.endTimer(), getsize(localPath))
        return dic_downloadData

    def DeleteContainer(self, container_name='blobtester'):
        self.startTimer()
        blobList = self.ListObjectsInContainer(container_name)
        for aBlob in blobList:
            aBlob.delete()
        bucket = self.storage_client.get_bucket(container_name)
        bucket.delete()
        return {container_name: self.endTimer(), 'operation':'Deleted'}
    


if __name__=="__main__":
    # For GCE, there are no credentials to read in. The sdk driver itself
    # uses the json credentials file pointed to by the 
    # GOOGLE_APPLICATION_CREDENTIALS OS environment variable
    print sys.argv
    gcpex = GCPExercizer()
    # Upload
    pickle.dump(gcpex.UploadObjectsToContainer(localDir = sys.argv[1]), open('/tmp/outputdata/objbench/gcp_upload.pkl','wb'))
    # Download
    pickle.dump(gcpex.DownloadObjectsFromContainer(localDir = sys.argv[1]), open('/tmp/outputdata/objbench/gcp_download.pkl','wb'))
    # Delete
    pprint(gcpex.DeleteContainer())
