from time import time
from sys import argv, exit, stderr
import numpy as np

# benchmarking parameters
dtype = 'int16'
maxVal = np.iinfo(dtype).max

reps = 3
shape = (10, 10)

try:
    framework = argv[1]
except:
    print("please specify a framework ('numpy', 'dask', or 'bolt') as the first argument", file=stderr)
    exit()

if  arrayType not in ['numpy', 'dask', 'bolt']:
    print("firt argument must be a valid framework: 'numpy', 'dask', or 'bolt'", file=stderr)

# array creation in each framework
if framework is 'numpy':
    def createArray(shape, dtype):
        return np.random.randint(low=0, high=maxVal+1, size=shape, dtype=dtype)

elif framework is 'dask':
    def createArray(shape, dtype):
        import dask.array as da
        return da.random.randing(low=0, high=maxVal+1, size=shape, dtype=dtype, chunks=(1, shape[1]))

elif framework is 'bolt':
    def createArray(shape, dtype):
        # check that SparkContext is available
        try:
            sc
        except NameError:
            print("script must be run with PySpark when using Bolt", file=stderr)
            exit()

        import thunder as td
        return td.series.fromrandom(shape, engine=sc, dtype=dtype).values

# array manipulation in each framework
# 1. subtract mean from first axis
# 2. subtract mean from second axis
# 3. compute mean across second axis 
if framework is 'numpy':
    def computeOnArray(arr):
        tmp = arr - arr.mean(axis=1, dtype=dtype)
        tmp = tmp - tmp.mean(axis=0, dtype=dtype)
        return tmp.sum(axis=0)

elif framework is 'dask':
    def computOnArray(arr):
        tmp = arr - arr.mean(axis=1, dtype=dtype)
        tmp = tmp.rechunk((shape[0], 1))
        tmp = tmp - tmp.mean(axis=0, dtype=dtype)
        return np.array(tmp.sum(axis=0))

elif framework is 'bolt':
    def computOneArray(arr):
        tmp = arr.map(lambda x: x - x.mean())
        tmp = tmp.swap((0,), (0,))
        tmp = arr.map(lambda x: x - x.mean())
        tmp.
