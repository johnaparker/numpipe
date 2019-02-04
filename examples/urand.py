import numpy as np
from random import random
from pinboard import pinboard
import os

### Setup
job = pinboard()

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
    job.add_instance(sim, f'S{i}', param=i)

job.execute()
