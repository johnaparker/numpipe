import numpipe
from numpipe import pbar
from time import sleep

### Setup
job = numpipe.scheduler()

import numpy as np
N = 100
T = 5

@job.cache
def progress(i):
    progress = 0
    for j in pbar(range(N)):
        if i in (2,5,8) and j == 40:
            raise RuntimeError
        sleep(T/N)
        yield dict()

for i in range(10):
    job.add(progress, i=i)

### execute
job.execute()
