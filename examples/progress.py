from pinboard import pinboard
from time import sleep
from tqdm import tqdm

### Setup
job = pinboard()

import numpy as np
x = np.zeros([1000, 1000], dtype=float)
N = 10
T = 5

@job.cache(iterations=N)
def progress(self):
    progress = 0

    for i in tqdm(self.iterations()):
        sleep(T/N)
        yield dict(counter=x)

### execute
job.execute()
