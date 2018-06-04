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

import os, time
from os.path import join, getsize ,exists
from traceback import format_exc
class Exercizer(object):
    def __init__(self):
        self.resetTimer()
    
    def makeOneRandomBinFile (self, 
        filePath ='/tmp/file',
        sizeinkb = 100,
        ):
        try:    
            with open(filePath, 'wb') as fout:
                fout.write(os.urandom(sizeinkb*1000)) 
        except:
            print "Error creating {} of size {}".format(filePath,sizeinkb)
            print format_exc()

    def makeRandomBinFiles (self, 
        outDir='/tmp/smalldir', 
        numFiles=10, 
        minSizekb=1, 
        maxSizekb = 1000001,
        fnPrefix = 'file_'):
        
        if not exists(outDir):
            os.makedirs(outDir)
        ctr = 1
        while ctr <= numFiles:
            try:
                fileSize = max( 1, ctr * (maxSizekb - minSizekb)/numFiles)
                self.makeOneRandomBinFile(join(outDir,fnPrefix + str(fileSize)), fileSize)
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

