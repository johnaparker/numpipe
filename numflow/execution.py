"""
Execution of functions
"""

import os
import h5py
import numpy as np
from numflow.fileio import load_symbols, write_symbols

class deferred_function:
    """wrapper around a function -- to defer its execution and store metadata"""
    def __init__(self, function, name, args=(), kwargs={}, num_iterations=None):
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.__name__ = function.__name__ 
        self.__doc__  = function.__doc__ 
        self.name = name

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

