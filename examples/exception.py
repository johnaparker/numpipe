from numflow import numflow
from time import sleep

### Setup
job = numflow()

N = 10
T = .5

@job.cache(iterations=N)
def first(self):
    for i in self.iterations():
        sleep(T/N)

    return {}

@job.cache
def second(self):
    x = 1/0
    return {}


@job.cache(iterations=N)
def third(self):
    for i in self.iterations():
        sleep(T/N)

    return {}

### execute
job.execute()
