"""
Python API to create jobs and register cached functions and at-end functions
"""

import argparse
import h5py
import os
import sys
import types
from h5cache import h5cache
from inspect import signature

class Bunch:
    """Simply convert a dictionary into a class with data members equal to the dictionary keys"""
    def __init__(self, adict):
        self.__dict__.update(adict)

def load_symbols(filepath):
    """Load all symbols from h5 filepath"""

    collection = {}
    with h5py.File(filepath, 'r') as f:
        for dset_name in f:
            collection[dset_name] = f[dset_name][...]

    return Bunch(collection)

def write_symbols(filepath, symbols):
    """Write all symbols to h5 file, where symbols is a {name: value} dictionary
       
       Arguments:
           filepath      path to file
           symbols       {name: vale} dictionary
    """
    with h5py.File(filepath, 'a') as f:
        for name,symbol in symbols.items():
            f[name] = symbol

class deferred_function:
    """wrapper around a function -- to defer its execution and store metadata"""
    def __init__(self, function, args=(), kwargs={}):
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.__name__ = function.__name__ 
        self.__doc__  = function.__doc__ 

    def __call__(self):
        return self.function(*self.args, **self.kwargs)

class target:
    """
    A target is the output of a cached function and determines whether it needs to be rerun
    It specifies the type of storage file
    """
    def __init__(self, filepath):
        self.filepath = filepath

    def load(self):
        """Load symbols"""
        return load_symbols(self.filepath)

    def write(self, symbols):
        """Write symbols"""
        write_symbols(self.filepath, symbols)

    def exists(self):
        """Return true if the target exists"""
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def remove(self):
        os.remove(self.filepath)

class pinboard:
    """Deferred function evaluation and access to cached function output"""

    def __init__(self):
        self.cached_functions = {}
        self.at_end_functions = {}
        self.targets = {}
        self.instances = {}
        self.instance_functions = {}

    def load(self, function=None):
        """
        Load cached symbols for particular function
        If function is None, read symbols for all functions
        """

        return self.targets[function.__name__].load()

    def defer_load(self, function=None):
        pass

    def execute(self, store=None):
        """Run the requested cached functions and at-end functions
           
           Arguments:
               store       {name: data} dictionary to write as additional data (optional)
        """
        cache_size = 1000
        chunk_size = 1000

        self._run_parser()

        ### display only event
        if self.args.action == 'display':
            self.display_functions()
            return

        ### write store to file
        if store is not None:
            with h5py.File('store.h5', 'w') as f:
                for name,value in store.items():
                    f[name] = value

        ### determine which functions to execute based on file and command line
        functions_to_execute = {}
        if self.args.rerun is None:
            for name,func in self.cached_functions.items():
                if not self.targets[name].exists():
                    functions_to_execute[name] = func

            for base,instances in self.instances.items():
                for name,instance in instances.items():
                    if not self.targets[name].exists():
                        functions_to_execute[name] = instance

        elif len(self.args.rerun) == 0:
            functions_to_execute.update(self.cached_functions)
            for instances in self.instances.values():
                functions_to_execute.update(instances)

        else:
            for name in self.args.rerun:
                if name in self.cached_functions.keys():
                    functions_to_execute[name] = self.cached_functions[name]
                elif name in self.instances.keys():
                    functions_to_execute.update(self.instances[name])
                else:
                    try:
                        base = name.split('-')[0]
                        functions_to_execute[name] = self.instances[base][name]
                    except KeyError:
                        raise ValueError(f"Invalid argument: function '{name}' does not correspond to any cached function")

        self._request_to_overwrite(names=functions_to_execute.keys())

        ### execute all items
        for name,func in functions_to_execute.items():
            print(f"Running cached function '{name}'")
            symbols = func()

            ### Generator functions
            if isinstance(symbols, types.GeneratorType):
                ### create all the caches based on the first set of symbols
                caches = {}
                next_symbols = next(symbols)
                for symbol_name, next_symbol in next_symbols.items():
                    caches[symbol_name] = h5cache(self.targets[name].filepath, '', symbol_name, next_symbol, chunk_size, cache_size)

                ### iterate over the remaining symbols, caching each one
                for next_symbols in symbols:
                    for symbol_name, next_symbol in next_symbols.items():
                        caches[symbol_name].add(next_symbol)

                ### empty any of the remaining cache
                for cache in caches.values():
                    cache.flush()

            ### Standard Functions
            else:
                if not isinstance(symbols, dict):
                    raise ValueError(f"Invalid return type: function '{name}' needs to return a dictionary of symbols")

                self._write_symbols(name, symbols)

        ### At-end functions
        if self.at_end_functions:
            print("Running at-end functions")
            for func in self.at_end_functions.values():
                func()

    def add_instance(self, name, func, *args, **kwargs):
        """
        Add an instance (a function with specified args and kwargs)
        """
        filepath = f'{func.__name__}-{name}.h5'
        func_name = f'{func.__name__}-{name}'
        
        self.targets[func_name] = target(filepath)
        self.instances[func.__name__][func_name] = deferred_function(func, args, kwargs)

    def cache(self, func):
        """decorator to add a cached function to be conditionally ran"""
        sig = signature(func)
        if not sig.parameters:
            self.cached_functions[func.__name__] = deferred_function(func)
            filepath = f'{func.__name__}.h5'
            self.targets[func.__name__] = target(filepath)
        else:
            self.instances[func.__name__] = {}
            self.instance_functions[func.__name__] = func

        return func

    def at_end(self, func):
        """decorator to add a function to be executed at the end"""
        self.at_end_functions[func.__name__] = deferred_function(func)
        return func

    def shared(self, class_type):
        """decorator to add a class for shared variables"""
        return class_type

    def display_functions(self):
        print("cached functions:")
        for name,func in self.cached_functions.items():
            print('\t', name, ' -- ', func.__doc__, sep='')

        for base,instance in self.instances.items():
            print('\t', base, ' -- ', self.instance_functions[base].__doc__, sep='')
            print('\t ', f'[{len(instance)} instances] ', end='')
            for name, func in instance.items():
                subname = name.split('-')[1]
                print(subname, end=' ')
            print('')

        print('\n', "at-end functions:", sep='')
        for name,func in self.at_end_functions.items():
            print('\t', name, ' -- ', func.__doc__, sep='')

    def _write_symbols(self, name, symbols):
        """write symbols to cache inside group"""
        self.targets[name].write(symbols)

    def _request_to_overwrite(self, names):
        """Request if existing hdf5 file should be overwriten

           Argumnets: 
               names        list of group names to check
        """
        data_to_delete = []
        
        if names:
            data_to_delete.extend(filter(lambda name: self.targets[name].exists(), names))

        if data_to_delete:
            summary = "The following cached data will be deleted:\n"
            for data in data_to_delete:
                summary += self.targets[data].filepath + '\n'

            if not self.args.force:
                print(summary)
                delete = input(f"Continue with job? (y/n) ")

                if delete != 'y':
                    sys.exit('Aborting...')

            for data in data_to_delete:
                self.targets[data].remove()

    def _run_parser(self):
        """parse user request"""
        parser = argparse.ArgumentParser()
        parser.add_argument('-r', '--rerun', nargs='*', type=str, default=None, help='re-run specific cached functions by name')
        parser.add_argument('-f', '--force', action='store_true', help='force over-write any existing cached data')

        subparsers = parser.add_subparsers(dest="action")
        display_parser = subparsers.add_parser('display', help='display available functions and descriptions')

        self.args = parser.parse_args()

if __name__ == "__main__":
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

    @job.cache
    def sim3():
        """construct a time-series"""
        for i in range(5):
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
