from numflow import numflow
from time import sleep
from tqdm import tqdm

### Setup
job = numflow()

import numpy as np
N = 100
T = 5

@job.cache
def progress(i):
    progress = 0

    for i in tqdm(range(N), position=i, desc=f'job {i}'):
        sleep(T/N)
        yield dict()

for i in range(10):
    job.add_instance(progress, i=i)

### execute
job.execute()
