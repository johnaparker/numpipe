"""
Python API to create jobs and register cached functions and at-end functions
"""

import argparse
import h5py
import os
import sys

class Bunch:
    """Simply convert a dictionary into a class with data members equal to the dictionary keys"""
    def __init__(self, adict):
        self.__dict__.update(adict)

def load_symbols(filepath, group_name=None):
    """Load all symbols from h5 filepath, group (all groups if None)"""

    collection = {}
    with h5py.File(filepath, 'r') as f:
        if group_name is None:
            for group_name in f:
                group = f[group_name]

                symbols = {}
                for dset_name in group:
                    symbols[dset_name] = group[dset_name][...]

                collection[group_name] = Bunch(symbols)
        else:
            group = f[group_name]
            for dset_name in group:
                collection[dset_name] = group[dset_name][...]

    return Bunch(collection)

def write_symbols(filepath, symbols, group=None):
    """Write all symbols to h5 file, where symbols is a {name: value} dictionary
       
       Arguments:
           filepath      path to file
           symbols       {name: vale} dictionary
           group         group to write symbols to (default: root)
    """
    if group is None:
        group = ''

    with h5py.File(filepath, 'a') as f:
        for name,symbol in symbols.items():
            f[f'{group}/{name}'] = symbol

class deferred_function:
    """wrapper around a function -- to defer its execution and store metadata"""
    def __init__(self, function, description=None, args=()):
        self.function = function
        self.description = description
        self.args = args

    def __call__(self):
        return self.function(*self.args)

class pinboard:
    """Deferred function evaluation and access to cached function output"""

    def __init__(self, filepath):
        """filepath to hdf5 file to cache data"""

        self.filepath = filepath
        self.cached_functions = {}
        self.at_end_functions = {}

    def load(self, function=None):
        """
        Load cached symbols for {}particular function
        If function is None, read symbols for all registered functions
        """
        if function is None:
            group = None
        else:
            group = function.__name__

        return load_symbols(self.filepath, group)

    def execute(self, store=None):
        """Run the requested cached functions and at-end functions
           
           Arguments:
               store       {name: data} dictionary to write as additional data (optional)
        """

        self._run_parser()

        if self.args.action == 'display':
            self.display_functions()
            return

        if store is not None:
            with h5py.File(self.filepath, 'a') as f:
                if 'store' in f:
                    del f['store']
                for name,value in store.items():
                    f[f'store/{name}'] = value

        functions_to_execute = {}
        with h5py.File(self.filepath, 'a') as f:
            if self.args.rerun is None:
                for name,func in self.cached_functions.items():
                    if name not in f:
                        functions_to_execute[name] = func

            elif len(self.args.rerun) == 0:
                functions_to_execute.update(self.cached_functions)

            else:
                for name in self.args.rerun:
                    if name not in self.cached_functions.keys():
                        raise ValueError(f"Invalid argument: function '{name}' does not correspond to any cached function")
                    functions_to_execute[name] = self.cached_functions[name]

            for name,func in functions_to_execute.items():
                if name in f:
                    self._request_to_overwrite(name)

            for name,func in functions_to_execute.items():
                print(f"Running cached function '{name}'")
                symbols = func()
                if not isinstance(symbols,dict):
                    raise ValueError(f"Invalid return type: function '{name}' needs to return a dictionary of symbols")
                self._write_symbols(symbols, name)

        if self.at_end_functions:
            print("Running at-end functions")
            for func in self.at_end_functions.values():
                func()

    def cache(self, func):
        """decorator to add a cached function to be conditionally ran"""
        self.cached_functions[func.__name__] = deferred_function(func, func.__doc__)
        return func

    def at_end(self, func):
        """decorator to add a function to be executed at the end"""
        self.at_end_functions[func.__name__] = deferred_function(func, func.__doc__)
        return func

    def shared(self, class_type):
        """decorator to add a class for shared variables"""
        return class_type

    def display_functions(self):
        print("cached functions:")
        for name,func in self.cached_functions.items():
            print('\t', name, ' -- ', func.description, sep='')

        print('\n', "at-end functions:", sep='')
        for name,func in self.at_end_functions.items():
            print('\t', name, ' -- ', func.description, sep='')

    def _write_symbols(self, symbols, group=None):
        """write symbols to cache inside group"""
        write_symbols(self.filepath, symbols, group)


    def _request_to_overwrite(self, group=None):
        """request if existing hdf5 file should be overwriten"""
        if group is None:
            group = ''
        
        with h5py.File(self.filepath, 'a') as f:
            if not self.args.force and os.path.exists(self.filepath) and group in f:
                delete = input(f"Do you really want to write over existing data in '{self.filepath}/{group}'? (y/n) ")
                if delete != 'y':
                    sys.exit('Aborting...')
            if group in f:
                del f[group]

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
    filepath = 'temp.h5'
    job = pinboard(filepath)

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

    @job.at_end
    def vis():
        """visualize the data"""
        sim = job.load()
        plt.plot(x,sim.sim1.y)
        plt.plot(x,sim.sim2.z)
        plt.show()

    ### execute
    job.execute(store={'x': x})
