from numpipe import scheduler, pbar
import numpy as np
from time import sleep

job = scheduler()

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

if __name__ == '__main__':
    job.execute()