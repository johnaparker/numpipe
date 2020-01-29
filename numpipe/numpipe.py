"""
Defines the scheduler class, which does the following:
    * keeps track of all cached functions
    * parse arguments when program is run
    * execute functions (possibly in parallel)
    * cache results as they come in
"""

import h5py
import os
import sys
import pathlib
from inspect import signature
from multiprocessing import Pool, Value
import threading
import socket
import pickle
import numpy as np
from mpi4py import MPI
import subprocess
from time import sleep, time
from functools import partial
import matplotlib.pyplot as plt

from numpipe import slurm, display, notify, mpl_tools
from numpipe.execution import deferred_function, target, block, execute_block
from numpipe.utility import doublewrap
from numpipe.parser import run_parser
from numpipe.networking import recv_msg,send_msg

USE_SERVER = False

class scheduler:
    """Deferred function evaluation and access to cached function output"""

    def __init__(self, dirpath=None):
        self.blocks = dict()
        self.instances = dict()
        self.at_end_functions = dict()

        if dirpath is None:
            self.dirpath = sys.path[0]
        else:
            if dirpath[0] not in ('/', '~', '$'):
                self.dirpath = os.path.join(sys.path[0], dirpath)
            else:
                self.dirpath = dirpath
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
        self.mpi_rank = MPI.COMM_WORLD.Get_rank()
        self.notifications = []

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

        if func_name in self.instances.keys():
            if instance is None:
                class load_next:
                    def __init__(self, labels, blocks):
                        self.length = len(labels)
                        self.labels = iter(labels)
                        self.blocks = blocks

                    def __len__(self): 
                        return self.length

                    def __iter__(self):
                        return self

                    def __next__(self):
                        label = next(self.labels)
                        name = label[label.find('-')+1:]
                        return (name, self.blocks[label].target.load())

                labels = self.get_labels(func_name)
                return load_next(labels, self.blocks)

            else:
                label = f'{func_name}-{instance}'
        else:
            label = func_name

        return self.blocks[label].target.load()

    def execute(self):
        """Run the requested cached functions and at-end functions"""
        self.args = run_parser()

        ### display only event
        if self.args.action == 'display':
            self.display_functions()
            return

        if self.args.action == 'clean':
            self.clean()
            return

        if self.args.delete is not None:
            self.delete()
            return

        import numpipe as nf
        nf._tqdm_mininterval = self.args.tqdm
        
        self.num_blocks_executed = 0
        if not self.args.at_end:
            ### determine which functions to execute based on file and command line
            if self.args.rerun is None:
                blocks_to_execute = {name: block for name, block in self.blocks.items() if not block.target.exists()}
            elif len(self.args.rerun) == 0:
                blocks_to_execute = self.blocks
            else:
                blocks_to_execute = dict()
                for name in self.args.rerun:
                    labels = self.get_labels(name)
                    blocks_to_execute.update({label: self.blocks[label] for label in labels})

            self.num_blocks_executed = len(blocks_to_execute)
            aborting = False
            if self.mpi_rank == 0:
                overwriten = self._overwrite([block.target for block in blocks_to_execute.values()])
                if not overwriten:
                    aborting = True
                    display.abort_message()
            aborting = MPI.COMM_WORLD.bcast(aborting, root=0)
            if aborting:
                return

            if self.args.action == 'slurm':
                slurm.create_lookup(self.filename, blocks_to_execute.keys())

                sbatch_filename = slurm.create_sbatch(self.filename, blocks_to_execute.keys(), 
                        time=self.args.time, memory=self.args.memory)
                wall_time = slurm.wall_time(self.args.time)

                display.slurm_message(sbatch_filename, wall_time, self.num_blocks_executed, self.args.no_submit)
                return

            ### execute all items
            t_start = time()
            num_exceptions = 0
            with Pool(processes=self.args.processes) as pool:
                results = dict()
                remaining = list(blocks_to_execute.keys())
                while remaining or results:
                    to_delete = []
                    for name in remaining:
                        block = blocks_to_execute[name]
                        if self.ready_to_run(block):
                            results[name] = pool.apply_async(execute_block, 
                                    (block, name, self.mpi_rank, self.instances, self.args.cache_time))
                            to_delete.append(name)

                    for name in to_delete:
                        remaining.remove(name)

                    to_delete = []
                    for name, result in results.items():
                        if result.ready():
                            try:
                                result.get()
                            except Exception as e:
                                num_exceptions += 1
                                print(e)  # failed simulation; print instead of abort

                            self.blocks[name].complete = True
                            to_delete.append(name)

                    for name in to_delete:
                        results.pop(name)

                    sleep(.1)

                if USE_SERVER:
                    t = threading.Thread(target=self.listening_thread) 
                    t.start()

                pool.close()
                pool.join()
                
                self.complete = True

                if blocks_to_execute and self.mpi_rank == 0:
                    self.notifications.append(partial(notify.send_finish_message,
                                                filename=self.filename, 
                                                njobs=len(blocks_to_execute),
                                                time=time() - t_start,
                                                num_exceptions=num_exceptions))
                
                if USE_SERVER:
                    t.join()
                    self.pipe.close()

        ### At-end functions
        if self.mpi_rank == 0:
            if self.at_end_functions and not self.args.no_at_end:
                display.at_end_message()

                for func in self.at_end_functions.values():
                    func()

    def ready_to_run(self, block):
        if block.dependencies is None:
            return True
        
        for func in block.dependencies:
            if not self.blocks[func.__name__].complete:
                return False
        
        return True

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

    # @static_vars(counter=0)
    def add_instance(self, func, instance_name=None, **kwargs):
        """
        Add an instance (a function with specified kwargs)
        """
        if instance_name is None:
            instance_name = str(len(self.instances[func.__name__]))

        block_name = f'{func.__name__}-{instance_name}'
        filepath = f'{self.dirpath}/{self.filename}-{block_name}.h5'
        
        self.blocks[block_name] = block(
                          deferred_function(func, kwargs=kwargs, num_iterations=None),
                          target(filepath))
        self.instances[func.__name__].append(block_name)

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
    def cache(self, func, depends=None):
        """decorator to add a cached function to be conditionally ran"""
        sig = signature(func)
        if len(sig.parameters) == 0:
            filepath = f'{self.dirpath}/{self.filename}-{func.__name__}.h5'
            self.blocks[func.__name__] = block(
                        deferred_function(func, num_iterations=None),
                        target(filepath),
                        dependencies=depends)
        else:
            self.instances[func.__name__] = []

        return func

    def at_end(self, func):
        """decorator to add a function to be executed at the end"""
        self.at_end_functions[func.__name__] = deferred_function(func)
        return func

    def plots(self, func):
        """decorator to add a function to be executed at the end for plotting purposes"""

        def wrap():
            show_copy = plt.show
            plt.show = lambda: None
            mpl_tools.set_theme(self.args.theme)

            func()
            if self.num_blocks_executed > 0 and self.mpi_rank == 0:
                self.notifications.append(partial(notify.send_images,
                                            filename=self.filename))

            self.send_notifications()
            if self.args.save != '':
                mpl_tools.save_figures(self.filename, self.args.save)

            plt.show = show_copy
            plt.show()

        self.at_end_functions[func.__name__] = deferred_function(wrap)

        return wrap

    def send_notifications(self):
        if self.mpi_rank == 0:
            t = threading.Thread(target=partial(notify.send_notifications, 
                                           notifications=self.notifications,
                                           delay=self.args.notify_delay))
            t.start()

    def shared(self, class_type):
        """decorator to add a class for shared variables"""
        return class_type

    def display_functions(self):
        if not self.mpi_rank == 0:
            return
        display.display_message(self.blocks, self.instances, self.at_end_functions)

    def _clean(self, filepaths):
        """clean a set of filepaths

           Argumnets: 
               filepaths      list of filepaths to hdf5 files
        """
        if not self.mpi_rank == 0:
            return

        if filepaths:
            if not self.args.force:
                delete = display.delete_message(filepaths)

                if not delete:
                    return False

            for filepath in filepaths:
                os.remove(filepath)

        return True

    def _overwrite(self, targets):
        """Request if existing hdf5 file should be overwriten, return True if data is deleted

           Argumnets: 
               targets        list of targets to delete
        """
        if not self.mpi_rank == 0:
            return

        targets_to_delete = list(filter(lambda t: t.exists(), targets))
        filepaths = [target.filepath for target in targets_to_delete]

        if filepaths:
            if not self.args.force:
                delete = display.delete_message(filepaths)

                if not delete:
                    return False

        for target in targets_to_delete:
            target.remove()

        return True

    def get_labels(self, name):
        """get a list of block labels for a given name"""
        if name in self.blocks.keys():
            return [name]
        elif name in self.instances.keys():
            return self.instances[name]
        elif name[-3:] == '.h5':
            actual_name = name[name.find('-')+1:-3]
            if actual_name in self.blocks.keys():
                return [actual_name]

        raise ValueError(f"Invalid argument: function '{name}' does not correspond to any cached function")

    def delete(self):
        """
        delete target data
        """
        targets_to_delete = []

        if len(self.args.delete) == 0:
            targets_to_delete.extend([block.target for block in self.blocks])
        else:
            for name in self.args.delete:
                labels = self.get_labels(name)
                targets_to_delete.extend([self.blocks[label].target for label in labels])

        if self.mpi_rank == 0:
            overwriten = self._overwrite(targets_to_delete)
            if not overwriten:
                display.abort_message()

        return

    def clean(self):
        pathlist = pathlib.Path(self.dirpath).glob(f'{self.filename}-*.h5')
        current = [block.target.filepath for block in self.blocks.values()]

        filepaths = []
        for path in pathlist:
            path_str = str(path)
            if path_str not in current:
                filepaths.append(path_str)

        confirm = self._clean(filepaths)
        if not confirm:
            display.abort_message()
