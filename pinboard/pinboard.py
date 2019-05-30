"""
Python API to create jobs and register cached functions and at-end functions
"""

import argparse
import h5py
import os
import sys
import pathlib
import types
from pinboard.h5cache import h5cache_from
from pinboard.networking import recv_msg,send_msg
from pinboard import slurm
from inspect import signature
import multiprocessing
from multiprocessing import Pool, Value
import threading
import traceback
from time import sleep
from functools import wraps
import socket
import pickle
import numpy as np
from mpi4py import MPI
import subprocess
from termcolor import colored

USE_SERVER = False

class once(dict):
    """identical to dict; used to yield something only once"""
    pass

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

def yield_traceback(f):
    def wrap_f(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            raise Exception("".join(traceback.format_exception(*sys.exc_info())))
    return wrap_f

class Bunch:
    """Simply convert a dictionary into a class with data members equal to the dictionary keys"""
    def __init__(self, adict):
        self.__dict__.update(adict)

    def __getitem__(self, key):
        return self.__dict__[key]

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

current_iteration = {}


class first_argument:
    def __init__(self, name, num_iterations=None):
        self.name = name
        self.num_iterations = num_iterations
        if num_iterations is not None:
            current_iteration[name] = Value('i', 0)

    def iterations(self):
        def gen():
            for i in range(self.num_iterations):
                yield current_iteration[self.name].value
                current_iteration[self.name].value += 1
        return gen()

class deferred_function:
    """wrapper around a function -- to defer its execution and store metadata"""
    def __init__(self, function, name, args=(), kwargs={}, num_iterations=None):
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.__name__ = function.__name__ 
        self.__doc__  = function.__doc__ 
        self.name = name

        self.arg = first_argument(name, num_iterations=num_iterations)

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

    def __init__(self, dirpath=None):
        self.cached_functions = dict()
        self.at_end_functions = dict()
        self.targets = dict()
        self.instances = dict()
        self.instance_functions = dict()
        self.instance_iterations = dict()
        self.instance_counts = dict() 

        self.dirpath = dirpath
        if dirpath is None:
            self.dirpath = sys.path[0]
        else:
            if dirpath[0] not in ('/', '~', '$'):
                self.dirpath = os.path.join(sys.path[0], dirpath)
            self.dirpath = os.path.expanduser(self.dirpath)
            self.dirpath = os.path.expandvars(self.dirpath)
            pathlib.Path(self.dirpath).mkdir(parents=False, exist_ok=True) 

        self.filename = os.path.splitext(os.path.basename(sys.argv[0]))[0]

        address = ('localhost', 6000)
        if USE_SERVER:
            self.pipe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.pipe.connect(address)
            send_msg(self.pipe, pickle.dumps(['new', 'ID']))

        self.complete = False
        self.rank = MPI.COMM_WORLD.Get_rank()

    #TODO implement load all, jdefer
    def load(self, function=None, instance=None, defer=False):
        """
        Load cached symbols for particular function

        Arguments:
            function     name of cached function (if None: load all cached functions)
            instance     name of instance (if None: load all instances)
            defer        If True, defer loading
        """

        func_name = function.__name__

        if func_name in self.instance_functions.keys():
            if instance is None:
                class load_next:
                    def __init__(self, instances, targets):
                        self.length = len(instances[func_name].keys())
                        self.instances = iter(instances[func_name].keys())
                        self.targets = targets

                    def __len__(self): 
                        return self.length

                    def __iter__(self):
                        return self

                    def __next__(self):
                        target_name = next(self.instances)
                        instance_name = target_name[target_name.find('-')+1:]
                        return (instance_name, self.targets[target_name].load())

                return load_next(self.instances, self.targets)

            else:
                target_name = f'{func_name}-{instance}'
        else:
            target_name = func_name

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
        
        if not self.args.at_end:
            ### determine which cahced data to delete
            functions_to_delete = dict()
            if self.args.delete is not None:
                if len(self.args.delete) == 0:
                    functions_to_delete.update(self.cached_functions)
                    for instances in self.instances.values():
                        functions_to_delete.update(instances)
                else:
                    for name in self.args.delete:
                        if name in self.cached_functions.keys():
                            functions_to_delete[name] = self.cached_functions[name]
                        elif name in self.instances.keys():
                            functions_to_delete.update(self.instances[name])
                        else:
                            try:
                                base = name.split('-')[0]
                                functions_to_delete[name] = self.instances[base][name]
                            except KeyError:
                                raise ValueError(f"Invalid argument: function '{name}' does not correspond to any cached function")

                if self.rank == 0:
                    overwriten = self._overwrite(names=functions_to_delete.keys())
                    if not overwriten:
                        print(colored('Aborting...', color='yellow', attrs=['bold']))

                return

            ### write store to file
            if store is not None:
                with h5py.File('store.h5', 'w') as f:
                    for name,value in store.items():
                        f[name] = value

            ### determine which functions to execute based on file and command line
            functions_to_execute = dict()
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
                    if name[-3:] == '.h5':
                        actual_name = name[name.find('-')+1:-3]
                    else:
                        actual_name = name

                    if actual_name in self.cached_functions.keys():
                        functions_to_execute[actual_name] = self.cached_functions[actual_name]
                    elif actual_name in self.instances.keys():
                        functions_to_execute.update(self.instances[actual_name])
                    else:
                        try:
                            base = actual_name.split('-')[0]
                            functions_to_execute[actual_name] = self.instances[base][actual_name]
                        except KeyError:
                            raise ValueError(f"Invalid argument: function '{actual_name}' does not correspond to any cached function")

            aborting = False
            if self.rank == 0:
                overwriten = self._overwrite(names=functions_to_execute.keys())
                if not overwriten:
                    aborting = True
                    print(colored('Aborting...', color='yellow', attrs=['bold']))
            aborting = MPI.COMM_WORLD.bcast(aborting, root=0)
            if aborting:
                return

            if self.args.action == 'slurm':
                ntasks = len(functions_to_execute)
                # slurm.create_lookup(self.filename, functions_to_execute.keys())
                sbatch_filename = slurm.create_sbatch(self.filename, functions_to_execute.keys(), 
                        time=self.args.time, memory=self.args.memory)

                wall_time = slurm.wall_time(self.args.time)
                print(colored('sbatch file', color='yellow', attrs=['bold']))
                subprocess.run(['cat',  f'{sbatch_filename}'])
                print(colored('\nSlurm job', color='yellow', attrs=['bold']))
                print('    Number of tasks:', colored(f'{ntasks}', attrs=['bold']))
                print('    Max wall-time:', colored(f'{wall_time:.2f} hours', attrs=['bold']))
                print('    Max CPU-hour usage:', colored(f'{ntasks*wall_time:.2f} hours', attrs=['bold']))

                if not self.args.no_submit:
                    submit_job = input(colored('\nSubmit Slurm job? (y/n) ', color='yellow', attrs=['bold']))
                    if submit_job != 'y':
                        print(colored('\nNot submitting Slurm job', color='yellow', attrs=['bold']))
                        return 

                    subprocess.run(['sbatch', sbatch_filename])
                    print(colored('\nSlurm job submitted', color='yellow', attrs=['bold']))

                return

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

                if USE_SERVER:
                    t = threading.Thread(target=self.listening_thread) 
                    t.start()

                for result in results:
                    try:
                        result.get()
                    except Exception as e:
                        print(e)  # failed simulation; print instead of abort

                pool.close()
                pool.join()
                
                self.complete = True
                
                if USE_SERVER:
                    t.join()
                    self.pipe.close()

        ### At-end functions
        if self.rank == 0:
            if self.at_end_functions and not self.args.no_at_end:
                print(colored('Running at-end functions', color='yellow'))
                for func in self.at_end_functions.values():
                    func()

    def listening_thread(self):
        while not self.complete:
            print('waiting...')
            request = recv_msg(self.pipe)
            print('received')

            if request == 'abort':
                return

            if request == 'progress':
                int_dict = {}
                for key,value in current_iteration.items():
                    int_dict[key] = value.value
                # acquire lock on pipe
                send_msg(self.pipe, pickle.dumps(int_dict))
                print('progress sent')

    # @yield_traceback
    def _execute_function(self, func, name):
        try:
            if self.rank == 0:
                print(colored(f"Running cached function '{name}'", color='yellow'))
            MPI.COMM_WORLD.Barrier()
            symbols = func()

            ### Generator functions
            if isinstance(symbols, types.GeneratorType):
                caches = dict()

                ### iterate over all symbols, caching each one
                for next_symbols in symbols:
                    if self.rank == 0:
                        if type(next_symbols) is once:
                            self._write_symbols(name, next_symbols)
                        else:
                            for symbol_name, next_symbol in next_symbols.items():
                                if symbol_name not in caches:
                                    caches[symbol_name] = h5cache_from(next_symbol, self.targets[name].filepath, symbol_name, cache_time=self.args.cache_time)
                                else:
                                    caches[symbol_name].add(next_symbol)

                ### empty any of the remaining cache
                if self.rank == 0:
                    for cache in caches.values():
                        cache.flush()

            ### Standard Functions
            else:
                if isinstance(symbols, dict):
                    self._write_symbols(name, symbols)
                elif symbols is None:
                    self._write_symbols(name, dict())
                else:
                    raise ValueError(f"Invalid return type: function '{name}' needs to return a dictionary of symbols")

        except:
            raise Exception(f"Cached function '{name}' failed:\n" + "".join(traceback.format_exception(*sys.exc_info())))

    # @static_vars(counter=0)
    def add_instance(self, func, instance_name=None, **kwargs):
        """
        Add an instance (a function with specified kwargs)
        """
        if instance_name is None:
            instance_name = str(self.instance_counts[func.__name__])
            self.instance_counts[func.__name__] += 1

        func_name = f'{func.__name__}-{instance_name}'
        filepath = f'{self.dirpath}/{self.filename}-{func_name}.h5'
        
        self.targets[func_name] = target(filepath)
        num_iterations = self.instance_iterations[func.__name__]
        self.instances[func.__name__][func_name] = deferred_function(func, func_name, kwargs=kwargs, num_iterations=num_iterations)

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
        if len(sig.parameters) == 0:
            self.cached_functions[func.__name__] = deferred_function(func, func.__name__, num_iterations=iterations)
            filepath = f'{self.dirpath}/{self.filename}-{func.__name__}.h5'
            self.targets[func.__name__] = target(filepath)
        else:
            self.instances[func.__name__] = {}
            self.instance_functions[func.__name__] = func
            self.instance_iterations[func.__name__] = iterations
            self.instance_counts[func.__name__] = 0

        return func

    def at_end(self, func):
        """decorator to add a function to be executed at the end"""
        self.at_end_functions[func.__name__] = deferred_function(func, func.__name__)
        return func

    def shared(self, class_type):
        """decorator to add a class for shared variables"""
        return class_type

    def display_functions(self):
        if not self.rank == 0:
            return

        print(colored("cached functions:", color='yellow', attrs=['bold']))
        for name,func in self.cached_functions.items():
            print('    ', colored(name, color='yellow'), ' -- ', func.__doc__, sep='')

        for base,instance in self.instances.items():
            print('    ', colored(base, color='yellow'), ' -- ', self.instance_functions[base].__doc__, sep='')
            print('      ', f'[{len(instance)} instances] ', end='')
            for name, func in instance.items():
                subname = name.split('-')[1]
                print(subname, end=' ')
            print('')

        print(colored("\nat-end functions:", color='yellow', attrs=['bold']))
        for name,func in self.at_end_functions.items():
            print('    ', colored(name, color='yellow'), ' -- ', func.__doc__, sep='')

    def _write_symbols(self, name, symbols):
        """write symbols to cache inside group"""
        if not self.rank == 0:
            return

        self.targets[name].write(symbols)

    def _overwrite(self, names):
        """Request if existing hdf5 file should be overwriten

           Argumnets: 
               names        list of group names to check
        """
        if not self.rank == 0:
            return

        data_to_delete = []
        
        if names:
            data_to_delete.extend(filter(lambda name: self.targets[name].exists(), names))

        if data_to_delete:
            summary = colored("The following cached data will be deleted:\n", color='yellow', attrs=['bold'])
            for data in data_to_delete:
                summary += self.targets[data].filepath + '\n'

            if not self.args.force:
                print(summary)
                delete = input(colored(f"Continue with job? (y/n) ", color='yellow', attrs=['bold']))
                print('')

                if delete != 'y':
                    return False

            for data in data_to_delete:
                self.targets[data].remove()

        return True

    def _run_parser(self):
        """parse user request"""
        parser = argparse.ArgumentParser()

        subparsers = parser.add_subparsers(dest="action")
        display_parser = subparsers.add_parser('display', help='display available functions and descriptions')
        slurm_parse = subparsers.add_parser('slurm', help='run on a system with the Slurm Workload Manager')

        for p in [parser, slurm_parse]:
            p.add_argument('-r', '--rerun', nargs='*', type=str, default=None, help='re-run specific cached functions by name')
            p.add_argument('-f', '--force', action='store_true', help='force over-write any existing cached data')
            p.add_argument('-d', '--delete', nargs='*', type=str, default=None, help='delete specified cached data')
            p.add_argument('--at-end', action='store_true', default=False, help="only run at_end functions")
            p.add_argument('--no-at-end', action='store_true', default=False, help="don't run at_end functions")
            p.add_argument('-np', '--processes', type=int, default=1, help='number of processes to use in parallel execution')
            p.add_argument('-ct', '--cache_time', type=float, default=300, help='time until data cached data is flushed to file')


        slurm_parse.add_argument('-t', '--time', type=str, default='36', help='maximum run-time for the Slurm job, formated as {hours}:{minutes}:{seconds} (minutes and seconds optional)')
        slurm_parse.add_argument('-m', '--memory', type=float, default=2, help='maximum memory per cpu for the Slurm job in GB')
        slurm_parse.add_argument('--batch', action='store_true', help='submit the Slurm job in batches')
        slurm_parse.add_argument('--no-submit', action='store_true', help="don't submit the Slurm job after creating sbatch files")

        self.args = parser.parse_args()
