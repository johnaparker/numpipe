"""
Code related to file io:
    * reading / writing symbols to the hdf5 file
"""

import h5py
from numpipe.utility import Bunch

def load_symbols(filepath):
    """Load all symbols from h5 filepath"""

    collection = {}
    args = {}
    with h5py.File(filepath, 'r') as f:
        for dset_name in f:
            if isinstance(f[dset_name], h5py.Group):
                continue
            collection[dset_name] = f[dset_name][...]

        bunch = Bunch(collection)
        if 'args' in f:
            for dset_name in f['args']:
                args[dset_name] = f['args'][dset_name][...]
            if args:
                bunch['args'] = Bunch(args)

    return bunch

def write_symbols(filepath, symbols):
    """Write all symbols to h5 file, where symbols is a {name: value} dictionary
       
       Arguments:
           filepath      path to file
           symbols       {name: vale} dictionary
    """
    with h5py.File(filepath, 'a') as f:
        for name,symbol in symbols.items():
            f[name] = symbol
