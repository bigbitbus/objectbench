import sys
from exercizer import Exercizer

ex = Exercizer()

ex.makeRandomBinFiles(numFiles = 2, minSizekb = 1, 
    maxSizekb = 101, outDir = sys.argv[1] )
ex.makeRandomBinFiles(numFiles = 2, minSizekb = 200, 
    maxSizekb = 10001, outDir = sys.argv[1])
ex.makeRandomBinFiles(numFiles = 0, minSizekb = 20000, 
    maxSizekb = 100001, outDir = sys.argv[1])
