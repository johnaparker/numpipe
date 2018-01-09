from pinboard import pinboard
from time import sleep

### Setup
job = pinboard()

N = 10
T = .5

@job.cache
def first():
    progress = 0

    for i in range(N):
        sleep(T/N)
        progress += 1
        print(progress)

    return {}

@job.cache
def second():
    x = 1/0
    return {}


@job.cache
def third():
    progress = 0

    for i in range(N):
        sleep(T/N)
        progress += 1
        print(progress)

    return {}

### execute
job.execute()
