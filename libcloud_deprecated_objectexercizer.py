from libcloud.storage.types import Provider, ContainerDoesNotExistError
from libcloud.storage.providers import get_driver
import os
from os.path import join, getsize
import time
from traceback import format_exc

def makeRandomBinFiles (outdir='/tmp/smalldir', 
    numFiles=10, 
    minSizekb=1, 
    maxSizekb = 1000001):

    ctr = 1
    while ctr <= numFiles:
        try:
            fileSize = max( 1, ctr * (maxSizekb - minSizekb)/numFiles)
            with open(join(outdir,'file_'+ str(fileSize)), 'wb') as fout:
                fout.write(os.urandom(fileSize)) 
            ctr = ctr + 1
        except:
            print "Error creating random files"
            print(format_exc())


def get_container(driver, container_name='somecontainer'):
    containers = driver.list_containers()
    for aContainer in containers:
        if aContainer.name == container_name:
            return aContainer
    return None

def objUploader (credentials = {'uid': 'access_key', 'secret': 'secret_key'}, 
    provider = Provider.S3, 
    container_name = 'blobtestcontainer', 
    rootDir='/tmp/smalldir/uploads'):    
        cls = get_driver(provider)
        driver = cls(credentials['uid'], credentials['secret'])
        container = get_container(driver, container_name)
        if container == None:
            print "Creating container {}".format(container_name)
            container = driver.create_container(container_name=container_name)

        dic_uploadData = {}
        for root, dirs, files in os.walk(rootDir):
            for name in files:
                filePath = join(root,name)
                startTime = time.time()                
                try:
                    with open(filePath, 'rb') as iterator:
                        obj = driver.upload_object_via_stream(iterator=iterator,
                                                            container=container,
                                                            object_name=filePath)
                    dic_uploadData[filePath] = (time.time() - startTime, getsize(filePath))
                except:
                    print ('Failure uploading {}'.format(filePath))
                    print (format_exc())
                    dic_uploadData[filePath] = -1
        return dic_uploadData

def objDownloader(driver, container, obj, destDir):
    objDl = driver.get_object(container_name=container.name,
                            object_name=obj.name)
    fileName = os.path.basename(objDl.name)
    path = os.path.join(join(destDir, fileName))
    objDl.download(destination_path=path, overwrite_existing=True)
    return (path, getsize(path))

def containerDownloader (credentials = {'uid': 'access_key', 'secret': 'secret_key'}, 
    provider = Provider.S3, 
    container_name = 'blobtestcontainer', 
    destDir='/tmp/smalldir/downloads'):
    """
    Download all objects from the container into the destDir.
    Keyword Arguments:
        credentials {dict} -- [secret key and access key] (default: {{'uid': 'access_key', 'secret': 'secret_key'}})
        provider {[type]} -- [Cloud provider] (default: {Provider.S3})
        container_name {str} -- [Name of the container] (default: {'blobtestcontainer'})
        destDir {str} -- [Download destination directory] (default: {'/tmp/smalldir/downloads'})
    
    Returns:
        [type] -- [description]
    """

    dic_downloadData = {}
    cls = get_driver(provider)
    driver = cls(credentials['uid'], credentials['secret'])
    container = get_container(driver, container_name)
    objects = container.list_objects()
    for obj in objects:
        startTime = time.time()
        path, size = objDownloader(driver, container, obj, destDir)
        dic_downloadData[path] = (time.time() - startTime, size)
    return dic_downloadData

def containerRemover(credentials = {'uid': 'access_key', 'secret': 'secret_key'}, 
    provider = Provider.S3, 
    container_name = 'blobtestcontainer' ):
    """
    Remove all objects from the container.
    
    Keyword Arguments:
        container_name {str} -- [Name of the container] (default: {'blobtestcontainer'})
    """
    dic_deleteData = {}
    cls = get_driver(provider)
    driver = cls(credentials['uid'], credentials['secret'], project='salttest-197409')

    try:
        container = get_container(driver, container_name)
        objects = container.list_objects()
        for obj in objects:
            startTime = time.time()
            driver.delete_object(obj)
            dic_deleteData[obj.name] = (time.time() - startTime)
        driver.delete_container(container)
    except:
        print "Container does not exist; or error in containerRemover"
        print format_exc()

    return dic_deleteData

if __name__=="__main__":
    #GCE
    #credentials = {'uid': os.environ['GCP_ACCESS_KEY'], 'secret': os.environ['GCP_SECRET_KEY'] }
    #AZURE
    credentials = {'uid': os.environ['AZBLOBACCOUNT'], 'secret': os.environ['AZBLOBKEY'] }
   # dic_deleteData = containerRemover(credentials, Provider.GOOGLE_STORAGE)
   #makeRandomBinFiles()

    dic_upData = objUploader(credentials, Provider.AZURE_BLOBS)
   # dic_dlData = containerDownloader(credentials, Provider.GOOGLE_STORAGE)
    
  #  print dic_deleteData
  #  print dic_upData
  #  print dic_dlData


    