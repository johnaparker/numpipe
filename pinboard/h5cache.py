import h5py
import numpy as np
import time

def auto_chunk_size(size_record):
    """
    Determine the optimal choice for number of chunks given the size of a record in bytes
    """
    if size_record > 1e7:
        return 1
    elif size_record > 1e6:
        return 10
    elif size_record > 1e4:
        return 100
    else:
        return 1000

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
    def __init__(self, shape, dtype, size='100M'):
        """
        Cache for a numpy array

        Arguments:
            shape       size of numpy array
            dtype       array datatype
            size        cache memory size
        """
        size_bytes = strformat_to_bytes(size)
        size_dtype = np.dtype(dtype).itemsize
        self.size_record = size_dtype*np.prod(shape)
        self.records = int(max(1, size_bytes // self.size_record))

        cache_shape = (self.records,) + shape

        self.current_record = 0
        self.shape = shape
        self.cache = np.zeros(cache_shape, dtype=dtype)

    def add(self, record):
        """add a record to the cache"""
        # require shape(record) == self.shape
        if self.is_full():
            raise RuntimeError('the cache is full and needs to be cleared')
            
        self.cache[self.current_record] = record
        self.current_record += 1

    def is_full(self):
        """return true if the cache is full"""
        return self.current_record == self.records

    def clear(self):
        """empty the cache (cached data will be overwritten in future adds)"""
        self.current_record = 0


def npcache_from(record, size='100M', cache_initial=True):
    """
    Create an npcache from a record (np.array)
    
    Arguments:
        record       numpy array or scalar
        size         cache memory size
        cache_initial  If True, cache the record passed in (default: True)
    """
    if isinstance(record, np.ndarray):
        cache = npcache(record.shape, record.dtype, size)
    else:
        dtype = type(record)
        cache = npcache((), dtype, size)

    if cache_initial:
        cache.add(record) 

    return cache

class h5cache:
    def __init__(self, filepath, name, shape, dtype, group='/', chunk_size=None, cache_size='100M', cache_time=300):
        """
        npcache with automatic output to hdf5 file

        Arguments:
            filepath     filepath to h5 file
            name         name of dataset inside group
            shape        record shape
            dtype        record datatype
            group        name of group in h5 file
            chunk_size   size of h5 chunks (default: attempts to choose best)
            cache_size   cache memory size (default: 100 MB)
            cache_time   time (in seconds) to hold the cache (default: 5 minutes)
        """
        self.filepath = filepath
        self.group = group
        self.name = name
        self.h5path = f'{group}/{name}'
        self.cache_size = cache_size
        self.cache_time = cache_time
        self.time_start = time.time()
        self.npcache = npcache(shape, dtype, cache_size)

        if chunk_size is None:
            self.chunk_size = auto_chunk_size(self.npcache.size_record)
        else:
            self.chunk_size = chunk_size


        with h5py.File(self.filepath, 'a') as f:
            dset = f.create_dataset(self.h5path, shape=(0,) + shape, chunks=(self.chunk_size,) + shape, maxshape=(None,) + shape, dtype=dtype)

    def add(self, record):
        """
        Add a record to the cached data

        Arguments:
            record      record to cache (must have same shape as original record)
        """
        if self.npcache.is_full():
            self.flush()
        elif time.time() - self.time_start > self.cache_time:
            self.flush()
            self.time_start = time.time() 

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

def h5cache_from(record, filepath, name, group='/', chunk_size=None, cache_size='100M', cache_time=300, cache_initial=True):
    """
    Create an h5cache from a record

    Arguments:
        record       record (np.array)
        filepath     filepath to h5 file
        name         name of dataset inside group
        group        name of group in h5 file
        chunk_size   size of h5 chunks (default: 1000)
        cache_size   cache memory size
        cache_time   time (in seconds) to hold the cache (default: 5 minutes)
        cache_initial    If True, cache the initial record (default: True)
    """

    if isinstance(record, np.ndarray):
        shape = record.shape
        dtype = record.dtype
    else:
        shape = ()
        dtype = type(record)

    cache = h5cache(filepath, name, shape, dtype, group, chunk_size, cache_size, cache_time)

    if cache_initial:
        cache.add(record)

    return cache
