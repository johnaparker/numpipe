from pinboard import pinboard
import numpy as np
import matplotlib.pyplot as plt

### Setup
job = pinboard()

### Fast, shared code goes here
x = np.linspace(0,1,10)

@job.shared
class variables():
    def __init__(self):
        self.x = np.linspace(0,1,10)

    def writer(self):
        pass

### Slow, sim-only code goes here and relevant data is written to file
@job.cache
def sim1():
    """compute the square of x"""
    # shared = job.get_shared()
    # shared.x

    y = x**2
    return {'y': y}

@job.cache
def sim2():
    """compute the cube of x"""
    z = x**3
    return {'z': z}

@job.cache(iterations=5)
def sim3(self):
    """construct a time-series"""
    for i in self.iterations():
        z = x*i + 1
        yield {'time_series': z, 'time': i}

@job.cache
def sim4(param):
    """sim depends on parameter"""
    x = np.array([1,2,3])
    return {'y': param*x} 

job.add_instance('p2', sim4, 2)
job.add_instance('p3', sim4, 3)
job.add_instance('p4', sim4, 4)

@job.at_end
def vis():
    """visualize the data"""
    cache = job.load(sim1)
    plt.plot(x, cache.y)
    cache = job.load(sim2)
    plt.plot(x, cache.z)
    plt.show()

### execute
job.execute(store={'x': x})
