import os, sys, time
from os.path import join, getsize
import google.cloud.storage
from exercizer import Exercizer
from traceback import format_exc
class GCPExercizer(Exercizer):
    def __init__(self):
        Exercizer.__init__(self)
        self.storage_client = google.cloud.storage.Client()

    def UploadObjectsToContainer(self, container_name='blobtester', localDir = '/tmp/smalldir'):
        try:
            container = self.storage_client.create_bucket(container_name)
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

    gcpex = GCPExercizer()
    # Upload
    print gcpex.UploadObjectsToContainer()
    # Download
    print gcpex.DownloadObjectsFromContainer()
    # Delete
    print gcpex.DeleteContainer()
