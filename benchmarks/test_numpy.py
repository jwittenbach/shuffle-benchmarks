import numpy as np
from time import time

# functions and parameters shared across scripts
import shared


minVal = np.iinfo(shared.dtype).min
maxVal = np.iinfo(shared.dtype).max

shape = (10, 10)

for i in range(shared.reps):
    arr = np.random.randint(minVal, maxVal, shape, dtype=shared.dtype)
    print('looping')
