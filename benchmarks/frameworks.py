from time import time
from sys import argv, exit, stderr
import numpy as np

# benchmarking parameters
dtype = 'int16'
maxVal = np.iinfo(dtype).max

reps = 3
shape = (10, 10)

# check input
try:
    framework = argv[1]
except:
    print("please specify a framework ('numpy', 'dask', or 'bolt') as the first argument", file=stderr)
    exit()

if  framework not in ['numpy', 'dask', 'bolt']:
    print("firt argument must be a valid framework: 'numpy', 'dask', or 'bolt'", file=stderr)

# for each framework, define how we create the array
# as well as how to perform the following computation:
# 1. subtract mean from first axis
# 2. subtract mean from second axis
# 3. compute sum across second axis
if framework == 'numpy':
    def createArray(shape, dtype):
        return np.random.randint(low=0, high=maxVal+1, size=shape, dtype=dtype)

    def computeOnArray(arr):
        tmp = arr - arr.mean(axis=1, dtype=dtype)
        tmp = tmp - tmp.mean(axis=0, dtype=dtype)
        return tmp.sum(axis=0)

elif framework == 'dask':
    def createArray(shape, dtype):
        import dask.array as da
        return da.random.randint(low=0, high=maxVal+1, size=shape, dtype=dtype, chunks=(1, shape[1]))

    def computOnArray(arr):
        tmp = arr - arr.mean(axis=1, dtype=dtype)
        tmp = tmp.rechunk((shape[0], 1))
        tmp = tmp - tmp.mean(axis=0, dtype=dtype)
        return np.array(tmp.sum(axis=0))

elif framework == 'bolt':
    def createArray(shape, dtype):
        # check that SparkContext is available
        try:
            sc
        except NameError:
            print("script must be run with PySpark when using Bolt", file=stderr)
            exit()

        import thunder as td
        return td.series.fromrandom(shape, engine=sc, dtype=dtype).values

    def computeOnArray(arr):
        tmp = arr.map(lambda x: x - x.mean())
        tmp = tmp.swap((0,), (0,))
        tmp = arr.map(lambda x: x - x.mean())
        return np.array(tmp.map(lambda x: x.sum()).collect())

# run the program
arr = createArray(shape, dtype)
res = computeOnArray(arr)

print(res)
