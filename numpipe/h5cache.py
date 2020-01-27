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

def strformat_to_bytes(strformat):
    """
    Convert a string (e.g. "5M") to number of bytes (int)
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
    def __init__(self, shape, dtype, size='10M'):
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

        data_shape = (self.records,) + shape

        self.current_record = 0
        self.shape = shape
        self.data = np.empty(data_shape, dtype=dtype)

    def add(self, record):
        """add a record to the cache"""
        # require shape(record) == self.shape
        if self.is_full():
            raise RuntimeError('the cache is full and needs to be cleared')
            
        self.data[self.current_record] = record
        self.current_record += 1

        return self.is_full()

    def is_full(self):
        """return true if the cache is full"""
        return self.current_record == self.records

    def clear(self):
        """empty the cache (cached data will be overwritten in future adds)"""
        self.current_record = 0

class h5cache:
    def __init__(self, filepath, cache_size='100M', cache_time=300):
        """
        dictionary of (label, numpy array) to be outputed to an hdf5 file

        Arguments:
            filepath     filepath to h5 file
            cache_size   cache memory size (default: 100 MB)
            cache_time   time (in seconds) to hold the cache (default: 5 minutes)
        """
        self.filepath   = filepath
        self.cache_size = strformat_to_bytes(cache_size)
        self.cache_time = cache_time
        self.time_start = time.time()

        self.cache = dict()
        self.h5path = dict()

    def add(self, name, record, group='/', chunk_size=None):
        """
        Add a record to the cached data

        Arguments:
            name         name of dataset inside group
            record       record to cache (must have same shape as original record)
            group        name of group in h5 file
            chunk_size   size of h5 chunks (default: attempts to choose best)
        """

        try:
            is_full = self.cache[name].add(record)
        except KeyError:
            record = np.asarray(record)
            shape = record.shape
            dtype = record.dtype

            if chunk_size is None:
                size_dtype = np.dtype(dtype).itemsize
                chunk_size = auto_chunk_size(size_dtype*np.prod(shape))

            h5path = f'{group}/{name}'

            with h5py.File(self.filepath, 'a') as f:
                dset = f.create_dataset(h5path, shape=(0,) + shape, chunks=(chunk_size,) + shape, maxshape=(None,) + shape, dtype=dtype)

            self.h5path[name] = h5path
            self.cache[name] = npcache(shape, dtype)

            is_full = self.cache[name].add(record)

        if is_full or (time.time() - self.time_start) > self.cache_time:
            self.flush()
            self.time_start = time.time() 

    def flush(self):
        """
        Flush all remaining cached data to h5 file
        """
        with h5py.File(self.filepath, 'a') as f:
            for name in self.cache.keys():
                dset = f[self.h5path[name]]
                cache = self.cache[name]

                dset.resize((dset.shape[0] + cache.current_record,) + cache.shape)
                dset[-cache.current_record:] = cache.data[:cache.current_record]

                cache.clear()
