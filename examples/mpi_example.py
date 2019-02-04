import numpy as np
from random import random
from pinboard import pinboard
import os
from mpi4py import MPI
import sys

### Setup
job = pinboard()

@job.cache
def sim():
    x = np.random.randint(0, 1000)

    size = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()
    name = MPI.Get_processor_name()

    print(f'I am process {rank} of {size} on {name}. I have x = {x}')

    return dict(x=x)

@job.at_end
def vis():
    var = job.load(sim)
    print(f'The stored value: x = {var.x}')

job.execute()
