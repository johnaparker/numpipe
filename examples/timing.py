from numpipe import scheduler
import numpy as np
from time import sleep

job = scheduler()
pbar = job.progress

@job.cache
def sim():
    for i in pbar(range(100)):
        yield dict(counter=i)
        sleep(1)

@job.cache
def other():
    for i in pbar(range(100)):
        yield dict(counter=i)
        sleep(1)

@job.at_end
def vis():
    var = job.load(sim)
    print(var.counter)

job.execute()
