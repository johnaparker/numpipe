from numpipe import scheduler, once
import numpy as np
import matplotlib.pyplot as plt

### Setup
job = scheduler()

### Fast, shared code goes here
x = np.linspace(0,1,10)

### Slow, sim-only code goes here and relevant data is written to file
@job.cache
def sim1():
    """compute the square of x"""
    y = x**2
    return {'y': y}

@job.cache
def sim2():
    """compute the cube of x"""
    z = x**3
    return {'z': z}

@job.cache
def sim3():
    """construct a time-series"""
    for i in range(5):
        z = x*i + 1
        yield {'time_series': z, 'time': i}

    yield once(xavg=np.average(x))

@job.cache
def sim4(param):
    """sim depends on parameter"""
    x = np.array([1,2,3])
    return {'y': param*x} 

@job.cache
def sim5():
    pass

job.add(sim4, 'A', param=2)
job.add(sim4, 'A', param=3)
job.add(sim4, 'A', param=4)
job.add(sim4, 'B', param=4)

@job.plots
def vis():
    """visualize the data"""
    cache = job.load(sim1)
    plt.plot(x, cache.y)
    cache = job.load(sim2)
    plt.plot(x, cache.z)

    for name, cache in job.load(sim4):
        print(f'{name} instance has y = {cache.y} with param = {cache.args.param}')
    # with job.load(sim4, defer=True) as cache:
    plt.show()

### execute
if __name__ == '__main__':
    job.run()
