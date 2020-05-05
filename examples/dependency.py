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

@job.cache(depends=A)
def C(i):
    sleep(1)
    return dict(x=2)

@job.cache(depends=C)
def D():
    sleep(1)
    return dict(x=2)

job.add(C, i=0)
job.add(C, i=1).depends(B)

if __name__ == '__main__':
    job.run()
