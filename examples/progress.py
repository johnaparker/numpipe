from pinboard import pinboard
from time import sleep

### Setup
job = pinboard()

N = 100
T = 30

@job.cache(iterations=N)
def progress(self):
    progress = 0

    for i in self.iterations():
        sleep(T/N)
        # print(self.current_iteration)

    return {}

### execute
job.execute()
