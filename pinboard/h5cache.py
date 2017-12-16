import h5py
import numpy as np

def write_over(group, name, data):
    """write over group[data] if it exists, otherwise create it
               group           group or file object
               name            name of dataset
               data            data to write
    """
    if name in group:
        group[name][...] = data
    else:
        group[name] = data

class h5cache:
    def __init__(self, shape, size, dtype):
        """cache a numpy array
                shape       size of numpy array
                size        number of records to be cached
                dtype       array datatype
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


