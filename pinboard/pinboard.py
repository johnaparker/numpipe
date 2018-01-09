"""
Python API to create jobs and register cached functions and at-end functions
"""

import argparse
import h5py
import os
import sys
import types
from pinboard.h5cache import h5cache
from inspect import signature
from multiprocessing import Pool
from time import sleep
from functools import wraps
import socket
import pickle

def doublewrap(f):
    """
    a decorator decorator, allowing the decorator to be used as:
    @decorator(with, arguments, and=kwargs)
    or
    @decorator
    """
    @wraps(f)
    def new_dec(*args, **kwargs):
        if len(args) == 2 and len(kwargs) == 0 and callable(args[1]):
            # actual decorated function
            return f(*args)
        else:
            # decorator arguments
            return lambda realf: f(args[0], realf, *args[1:], **kwargs)

    return new_dec

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
    def __init__(self, function, args=(), kwargs={}, iterations=None):
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.__name__ = function.__name__ 
        self.__doc__  = function.__doc__ 

        self.iterations = iterations
        self.current_iteration = 0

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
        self.instance_iterations = {}

        address = ('localhost', 6000)
        self.pipe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.pipe.connect(address)
        self.pipe.sendall(pickle.dumps(['batch', 'ID']))

    #TODO implement load all, jdefer
    def load(self, function=None, instance=None, defer=False):
        """
        Load cached symbols for particular function

        Arguments:
            function     name of cached function (if None: load all cached functions)
            instance     name of instance (if None: load all instances)
            defer        If True, defer loading
        """

        target_name = function.__name__

        if function.__name__ in self.instance_functions.keys():
            if instance is None:
                #load all instances
                pass
            else:
                target_name += f'-{instance}'

        return self.targets[target_name].load()


    def execute(self, store=None):
        """Run the requested cached functions and at-end functions
           
           Arguments:
               store       {name: data} dictionary to write as additional data (optional)
        """

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
        with Pool(processes=self.args.processes) as pool:
            # for name, func in functions_to_execute.items():
                # pool.apply_async(self._execute_function, (func,name))


            results = [pool.apply_async(self._execute_function, (func,name)) for name,func in functions_to_execute.items()]
            # while results:
                # for result in results:
                    # if result.ready():
                        # result.get()
                        # results.remove(result)
                    # else:
                        # print('progress')
                # sleep(.1)
            for result in results:
                result.get()
            pool.close()
            pool.join()

        ### At-end functions
        if self.at_end_functions:
            print("Running at-end functions")
            for func in self.at_end_functions.values():
                func()

    def _execute_function(self, func, name):
        cache_size = 1000
        chunk_size = 1000

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

    def add_instance(self, func, name, *args, **kwargs):
        """
        Add an instance (a function with specified args and kwargs)
        """
        filepath = f'{func.__name__}-{name}.h5'
        func_name = f'{func.__name__}-{name}'
        
        self.targets[func_name] = target(filepath)
        iterations = self.instance_iterations[func.__name__]
        self.instances[func.__name__][func_name] = deferred_function(func, args, kwargs, iterations=iterations)

    def add_instances(self, func, instances):
        """
        Add multiple instances

        Arguments:
            func        cached function
            instances   dictionary of name: dict(kwargs)
        """
        for instance_name, kwargs in instances.items():
            self.add_instance(func, instance_name, **kwargs)

    @doublewrap
    def cache(self, func, iterations=None):
        """decorator to add a cached function to be conditionally ran"""
        sig = signature(func)
        if not sig.parameters:
            self.cached_functions[func.__name__] = deferred_function(func, iterations=iterations)
            filepath = f'{func.__name__}.h5'
            self.targets[func.__name__] = target(filepath)
        else:
            self.instances[func.__name__] = {}
            self.instance_functions[func.__name__] = func
            self.instance_iterations[func.__name__] = iterations

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
        parser.add_argument('-np', '--processes', type=int, default=1, help='number of processes to use in parallel execution')

        subparsers = parser.add_subparsers(dest="action")
        display_parser = subparsers.add_parser('display', help='display available functions and descriptions')

        self.args = parser.parse_args()
