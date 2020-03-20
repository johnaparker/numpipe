from numpipe import scheduler
from time import sleep

### Setup
job = scheduler()

N = 10
T = .5

@job.cache()
def first():
    for i in range(N):
        sleep(T/N)

    return {}

@job.cache
def second():
    x = 1/0
    return {}


@job.cache()
def third():
    for i in range(N):
        sleep(T/N)

    return {}

if __name__ == '__main__':
    job.execute()
