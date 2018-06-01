import os, time
from os.path import join, getsize
class Exercizer(object):
    def __init__(self):
        self.resetTimer()

    def makeRandomBinFiles (self, outdir='/tmp/smalldir', numFiles=10, 
        minSizekb=1, maxSizekb = 1000001):
        while ctr <= numFiles:
            try:
                fileSize = max( 1, ctr * (maxSizekb - minSizekb)/numFiles)
                with open(join(outdir,'file_'+ str(fileSize)), 'wb') as fout:
                    fout.write(os.urandom(fileSize)) 
                ctr = ctr + 1
            except:
                print "Error creating random files"
                print(format_exc())

    def resetTimer(self):
        self.startTime = -1
    
    def startTimer(self): 
        assert(self.startTime == -1)
        self.startTime = time.time()
    
    def endTimer(self):
        assert(self.startTime != -1)
        timeElapsed = time.time() - self.startTime
        self.startTime = -1
        return (timeElapsed)

