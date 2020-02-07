from numpipe import scheduler
from time import sleep

job = scheduler()

@job.cache()
def A():
    sleep(1)
    return dict(x=2)

@job.cache(depends=A)
def B():
    sleep(1)
    return dict(x=2)

@job.cache(depends=B)
def C():
    sleep(1)
    return dict(x=2)

job.execute()
