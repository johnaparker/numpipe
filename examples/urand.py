import numpy as np
from random import random
from numpipe import scheduler
import os

### Setup
job = scheduler()

@job.cache
def sim(param):
    x = np.random.random(5)

    return dict(x=x)

@job.at_end
def vis():
    for i in range(5):
        var = job.load(sim, f'S{i}')
        print(var.x)

for i in range(5):
    job.add(sim, f'S{i}', param=i)

if __name__ == '__main__':
    job.run()
