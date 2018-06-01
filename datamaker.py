from exercizer import Exercizer

ex = Exercizer()
ex.makeRandomBinFiles(numFiles = 25, minSizekb = 1, maxSizekb = 101)
ex.makeRandomBinFiles(numFiles = 20, minSizekb = 200, maxSizekb = 10001)
ex.makeRandomBinFiles(numFiles = 10, minSizekb = 20000, maxSizekb = 100001)
