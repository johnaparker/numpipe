import h5py
import numpy as np

def write_over(group, name, data):
    """
    Write over group[data] if it exists, otherwise create it

    Arguments:
        group           group or file object
        name            name of dataset
        data            data to write
    """
    if name in group:
        group[name][...] = data
    else:
        group[name] = data

def strformat_to_bytes(strformat):
    """
    Convert a string (like "5M") to number of bytes (int)
    """

    suffixes = {'B': 1}
    suffixes['K'] = 1000*suffixes['B']
    suffixes['M'] = 1000*suffixes['K']
    suffixes['G'] = 1000*suffixes['M']
    suffixes['T'] = 1000*suffixes['G']
    
    suffix = strformat[-1] 
    amount = int(strformat[:-1])
    return amount*suffixes[suffix]

class npcache:
    def __init__(self, shape, dtype, size=1000):
        """
        Cache a numpy array

        Arguments:
            shape       size of numpy array
            dtype       array datatype
            size        number of records to be cached
        """
        cache_shape = (size,) + shape

        self.size = size
        self.current_record = 0
        self.shape = shape
        self.cache = np.zeros(cache_shape, dtype=dtype)

    def add(self, record):
        """add a record to the cache"""
        # require shape(record) == self.shape
        self.cache[self.current_record] = record
        self.current_record += 1

    def is_full(self):
        """return true if the cache is full"""
        return self.current_record == self.size

    def clear(self):
        """empty the cache (cached data will be overwritten in future adds)"""
        self.current_record = 0


def npcache_from(record, cache_size=1000):
    """
    Create an npcache from a record
    
    Arguments:
        record       numpy array or scalar
        cache_size   size of record cache (default: 1000)
    """
    if isinstance(record, np.ndarray):
        cache = npcache(record.shape, record.dtype, cache_size)
    else:
        dtype = type(record)
        cache = npcache((), dtype, cache_size)

    cache.add(record) 
    return cache

class h5cache:
    def __init__(self, filepath, group, name, record, chunk_size=1000, cache_size=1000):
        """
        npcache with automatic output to hdf5 file

        Arguments:
            filepath     filepath to h5 file
            group        name of group in h5 file
            name         name of dataset inside group
            record       first record to write to dataset (numpy array or scalar)
            chunk_size   size of h5 chunks (default: 1000)
            cache_size   size of record cache (default: 1000)
        """
        self.filepath = filepath
        self.group = group
        self.name = name
        self.h5path = f'{group}/{name}'
        self.chunk_size = chunk_size
        self.cache_size = cache_size

        with h5py.File(self.filepath, 'a') as f:
            self.npcache = npcache_from(record, cache_size)
            dtype = self.npcache.cache.dtype
            shape = self.npcache.shape
            dset = f.create_dataset(self.h5path, shape=(0,) + shape, chunks=(self.chunk_size,) + shape, maxshape=(None,) + shape, dtype=dtype)

    def add(self, record):
        """
        Add a record to the cached data

        Arguments:
            record      record to cache (must have same shape as original record)
        """
        if self.npcache.is_full():
            self.flush()
        self.npcache.add(record)

    def flush(self):
        """
        Flush all remaining cached data to h5 file
        """
        with h5py.File(self.filepath, 'a') as f:
            dset = f[self.h5path]
            dset.resize((dset.shape[0]+self.npcache.current_record,) + self.npcache.shape)
            dset[-self.npcache.current_record:] = self.npcache.cache[:self.npcache.current_record]
        self.npcache.clear()

