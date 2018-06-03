import sys
from exercizer import Exercizer

ex = Exercizer()

ex.makeRandomBinFiles(numFiles = 50, minSizekb = 1, 
    maxSizekb = 101, outDir = sys.argv[1], fnPrefix = sys.argv[2] )
ex.makeRandomBinFiles(numFiles = 40, minSizekb = 200, 
    maxSizekb = 10001, outDir = sys.argv[1], fnPrefix = sys.argv[2])
ex.makeRandomBinFiles(numFiles = 10, minSizekb = 20000, 
    maxSizekb = 100001, outDir = sys.argv[1], fnPrefix = sys.argv[2])


