"""
Execution of functions
"""

import os
import sys
import h5py
import numpy as np
from typing import Iterable
import traceback
import types
from functools import partial

import numpipe
from numpipe.fileio import load_symbols, write_symbols
from numpipe.h5cache import h5cache
from numpipe.utility import once
from numpipe import display, config

def is_windows():
    platform = sys.platform
    if 'win' in platform.lower():
        return True
    else:
        return False

class deferred_function:
    """wrapper around a function -- to defer its execution and store metadata"""
    def __init__(self, function, args=(), kwargs={}, num_iterations=None):
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.__name__ = function.__name__ 
        self.__doc__  = function.__doc__ 

        # self.arg = first_argument(name, num_iterations=num_iterations)

    def __call__(self):
        # return self.function(self.arg, *self.args, **self.kwargs)
        np.random.seed(int.from_bytes(os.urandom(4), byteorder='little'))
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

    def write_args(self, symbols):
        """Write instance argument symbols to args group"""
        with h5py.File(self.filepath, 'a') as f:
            g = f.require_group('args')
            for name,symbol in symbols.items():
                try:
                    g[name] = symbol
                except TypeError:
                    continue

    def exists(self):
        """Return true if the target exists"""
        if os.path.isfile(self.filepath):
            return True
        else:
            return False

    def remove(self):
        os.remove(self.filepath)

class block:
    """
    A (execution) block consists of a deffered function, a target, and optional dependencies
    """
    def __init__(self, deferred_function, target, dependencies=None):
        self.deferred_function = deferred_function
        self.target = target

        self.dependencies = []
        self.children = []
        self.complete = False

        self.depends(dependencies)

    def depends(self, dependencies):
        if dependencies is not None:
            f = lambda D: D if isinstance(D, str) else D.__name__
            if isinstance(dependencies, str) or not isinstance(dependencies, Iterable):
                new_deps = [f(dependencies)]
            else:
                new_deps = [f(D) for D in dependencies]

            self.dependencies.extend(new_deps)

# @yield_traceback
def execute_block(block, name, instances, cache_time, number, total):
    desc = f'({1+number}/{total}) {name}'
    numpipe._pbars.set_desc(desc)
    numpipe._pbars.make_placeholder()
    if is_windows():
        numpipe._pbars.set_njobs(total)

    cache = None
    try:
        func = block.deferred_function
        if func.__name__ in instances and name in instances[func.__name__]:
            ### write arguments if instance funcitont 
            block.target.write_args(func.kwargs)

        symbols = func()

        ### Generator functions
        if isinstance(symbols, types.GeneratorType):
            cache = h5cache(block.target.filepath, cache_time=cache_time)

            ### iterate over all symbols, caching each one
            for next_symbols in symbols:
                if type(next_symbols) is once:
                    block.target.write(next_symbols)
                else:
                    cache.add(next_symbols)

            ### empty any of the remaining cache
            cache.flush()

        ### Standard Functions
        else:
            if isinstance(symbols, dict):
                block.target.write(symbols)
            elif symbols is None:
                block.target.write(dict())
            else:
                raise ValueError(f"Invalid return type: function '{name}' needs to return a dictionary of symbols")

    except:
        if cache is not None:
            cache.flush()
        numpipe._pbars.fail_bar()
        raise Exception(f"Cached function '{name}' failed:\n" + "".join(traceback.format_exception(*sys.exc_info())))

    with numpipe._pbars.lock:
        numpipe._pbars.finish_bar()

def execute_block_debug(block, name, instances, cache_time, number, total):
    desc = f'({1+number}/{total}) {name}'
    numpipe._pbars.set_desc(desc)
    numpipe._pbars.make_placeholder()

    try:
        func = block.deferred_function
        if func.__name__ in instances and name in instances[func.__name__]:
            ### write arguments if instance funcitont 
            block.target.write_args(func.kwargs)

        symbols = func()

        ### Generator functions
        if isinstance(symbols, types.GeneratorType):
            cache = h5cache(block.target.filepath, cache_time=cache_time)

            ### iterate over all symbols, caching each one
            for next_symbols in symbols:
                if type(next_symbols) is once:
                    block.target.write(next_symbols)
                else:
                    cache.add(next_symbols)

            ### empty any of the remaining cache
            cache.flush()

        ### Standard Functions
        else:
            if isinstance(symbols, dict):
                block.target.write(symbols)
            elif symbols is None:
                block.target.write(dict())
            else:
                raise ValueError(f"Invalid return type: function '{name}' needs to return a dictionary of symbols")

    except Exception as err:
        numpipe._pbars.fail_bar()
        raise err

    with numpipe._pbars.lock:
        numpipe._pbars.finish_bar()
