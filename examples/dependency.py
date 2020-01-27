from numflow import scheduler
from time import sleep

job = scheduler()

@job.cache()
def A():
    print('running A')
    sleep(1)
    return dict(x=2)

@job.cache(depends=A)
def B():
    print('running B')
    sleep(1)
    return dict(x=2)

@job.cache(depends=A)
def C():
    print('running C')
    sleep(1)
    return dict(x=2)

job.execute()
