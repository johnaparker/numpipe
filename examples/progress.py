import numpipe
from time import sleep

### Setup
job = numpipe.scheduler()

import numpy as np
N = 100
T = 5

@job.cache
def progress(i):
    progress = 0
    for i in numpipe.tqdm(range(N)):
        sleep(T/N)
        yield dict()

for i in range(10):
    job.add_instance(progress, i=i)

### execute
job.execute()
