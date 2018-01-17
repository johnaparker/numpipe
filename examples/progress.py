from pinboard import pinboard
from time import sleep

### Setup
job = pinboard()

N = 100
T = 15

@job.cache(iterations=N)
def progress(self):
    progress = 0

    for i in self.iterations():
        sleep(T/N)
        print(i)

    return {}

### execute
job.execute()
