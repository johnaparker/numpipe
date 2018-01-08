from pinboard import pinboard
from time import sleep

### Setup
job = pinboard()

N = 100
T = 3

@job.cache
def progress():
    progress = 0

    for i in range(N):
        sleep(T/N)
        progress += 1
        print(progress)

    return {}

### execute
print('hi')
job.execute()
