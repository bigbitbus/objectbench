import os, sys, time
from os.path import join, getsize
from azure.storage.blob import BlockBlobService
from exercizer import Exercizer

class AzureExercizer(Exercizer):
    def __init__(self, env_credentials):
        Exercizer.__init__(self)
        self.blob_service = BlockBlobService(
            os.environ.get(env_credentials['account']), 
            os.environ.get(env_credentials['secret']))

    def UploadObjectsToContainer(self, container_name='blobtester', localDir = '/tmp/smalldir'):
        self.blob_service.create_container(container_name)
        dic_uploadData = {}
        for root, dirs, files in os.walk(localDir):
            for name in files:
                filePath = join(root,name)
                self.startTimer()
                try:
                    self.blob_service.create_blob_from_path(container_name, 
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
        return self.blob_service.list_blobs(container_name)
        
    def DownloadObjectsFromContainer(self, container_name = 'blobtester', localDir = '/tmp/smalldir'):
        dic_downloadData = {}
        self.startTimer()
        blobListGenerator = self.ListObjectsInContainer(container_name)
        dic_downloadData['_listObjectsInContainer'] = (self.endTimer(), "Container objects listing time") 
        for aBlob in blobListGenerator:
            self.startTimer()
            localPath = join(localDir,aBlob.name)
            self.blob_service.get_blob_to_path(container_name, aBlob.name, localPath )
            dic_downloadData[localPath] = (self.endTimer(), getsize(localPath))
        return dic_downloadData

    def DeleteContainer(self, container_name='blobtester'):
        self.startTimer()
        self.blob_service.delete_container(container_name)
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
