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

