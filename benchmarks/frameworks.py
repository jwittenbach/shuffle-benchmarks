from time import time
from sys import argv, exit, stderr
import numpy as np

# benchmarking parameters
reps = 1
dims = [np.round(np.sqrt(5)**k).astype(int) for k in range(2, 15)]
sizes = [8*d**2 for d in dims]
print(dims)
print(sizes)

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
    maxSize = 1e9

    def createArray(dim):
        np.random.seed(0)
        return np.random.normal(size=(dim, dim))

    def computeOnArray(arr):
        tmp = arr - arr.mean(axis=1)[:, np.newaxis]
        return tmp.mean(axis=0)

elif framework == 'dask':
    maxSize = 1e6

    def createArray(dim):
        import dask.array as da
        da.random.seed(0)
        return da.random.normal(size=(dim, dim), chunks=(1, shape[1]))

    def computeOnArray(arr):
        tmp = arr - arr.mean(axis=1)[:, np.newaxis]
        #tmp = tmp.rechunk((shape[0], 1))
        return np.array(tmp.mean(axis=0))

elif framework == 'bolt':
    maxSize = 1e6

    from pyspark import SparkConf, SparkContext
    conf = SparkConf().setAppName('array-benchmark')
    sc = SparkContext(conf=conf)

    def createArray(dim):
        import thunder as td
        return td.series.fromrandom((dim, dim), engine=sc, npartitions=shape[0]).values

    def computeOnArray(arr):
        tmp = arr.map(lambda x: x - x.mean())
        tmp = tmp.swap((0,), (0,))
        return tmp.map(lambda x: x.mean()).toarray()

# run the program
results = []
for size, dim in zip(sizes, dims):

    if size > maxSize:
        break

    l = []
    for r in range(reps):
        start = time()
        arr = createArray(dim)
        res = computeOnArray(arr)
        stop = time()
        l.append(stop - start)
    results.append(l)

print(results)
