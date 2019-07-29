"""
Utility functions and classes
"""

from functools import wraps
import traceback

class once(dict):
    """identical to dict; used to yield something only once"""
    pass

class Bunch:
    """convert a dictionary into a class with data members equal to the dictionary keys"""
    def __init__(self, adict):
        self.__dict__.update(adict)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

def doublewrap(func):
    """
    a decorator decorator, can be used as @decorator(...) or @decorator
    """

    @wraps(func)
    def new_func(*args, **kwargs):
        if len(args) == 2 and len(kwargs) == 0 and callable(args[1]):
            return func(*args)
        else:
            return lambda f: func(args[0], f, *args[1:], **kwargs)

    return new_func

def yield_traceback(func):
    """decorator to properly yield the traceback of a function in a parallel environment"""

    def new_func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            raise Exception("".join(traceback.format_exception(*sys.exc_info())))

    return new_func

class first_argument:
    # current_iteration = {}
    def __init__(self, name, num_iterations=None):
        self.name = name
        self.num_iterations = num_iterations
        if num_iterations is not None:
            current_iteration[name] = Value('i', 0)

    def iterations(self):
        def gen():
            for i in range(self.num_iterations):
                yield current_iteration[self.name].value
                current_iteration[self.name].value += 1
        return gen()
