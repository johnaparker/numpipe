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
import logging
from inspect import signature
from multiprocessing import Pool, Value
import threading
import socket
import pickle
import numpy as np
import subprocess
from time import sleep, time
from functools import partial
from typing import Iterable, types
import matplotlib.pyplot as plt
import traceback
from copy import copy
import itertools
import warnings

import numpipe
from numpipe import slurm, display, notify, mpl_tools, config
from numpipe.execution import deferred_function, target, block, execute_block, execute_block_debug
from numpipe.utility import doublewrap
from numpipe.parser import run_parser
from numpipe.networking import recv_msg,send_msg

USE_SERVER = False

class scheduler:
    """Deferred function evaluation and access to cached function output"""

    def __init__(self, dirpath=None):
        warnings.simplefilter("default")

        self.blocks = dict()
        self.instances = dict()
        self.instance_counts = dict()
        self.instance_dependency = dict()
        self.at_end_functions = dict()
        self.animations = dict() 

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


        if USE_SERVER:
            address = ('localhost', 6000)
            self.pipe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.pipe.connect(address)
            send_msg(self.pipe, pickle.dumps(['new', 'ID']))

        self.complete = False
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
        if not isinstance(instance, str) and isinstance(instance, Iterable):
            instance = '-'.join([str(x) for x in instance])

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
        warnings.warn('use scheduler.run() instead of scheduler.execute()', DeprecationWarning)
        self.run()

    def run(self):
        """Run the requested cached functions and at-end functions"""
        self._init_logging()
        self.args = run_parser()
        numpipe._pbars.mininterval = self.args.mininterval
        numpipe._pbars.character = config.get_config()['progress']['character']
        self.fix_block_names()

        if self.args.notify_message is not None:
            self.notifications.append(partial(notify.send_message_from, self.args.notify_message, self.filename))

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

            for name in self.args.exclude:
                for key in self.get_labels(name):
                    blocks_to_execute.pop(key, 0)

            self.resolve_dependencies_down(blocks_to_execute)
            self.num_blocks_executed = len(blocks_to_execute)

            overwriten = self._overwrite([block.target for block in blocks_to_execute.values()])
            if not overwriten:
                display.abort_message()
                return

            self.resolve_dependencies_up(blocks_to_execute)

            if self.args.action == 'slurm':
                slurm.create_lookup(self.filename, blocks_to_execute.keys())

                sbatch_filename = slurm.create_sbatch(self.filename, blocks_to_execute.keys(), 
                        time=self.args.time, memory=self.args.memory)
                wall_time = slurm.wall_time(self.args.time)

                display.slurm_message(sbatch_filename, wall_time, self.num_blocks_executed, self.args.no_submit)
                return

            ### execute all items
            if self.args.processes is None:
                nprocs = min(os.cpu_count(), self.num_blocks_executed)
            else:
                nprocs = min(self.args.processes, self.num_blocks_executed)

            numpipe._pbars.set_njobs(self.num_blocks_executed)

            t_start = time()
            if self.num_blocks_executed:
                display.cached_function_message()

                if self.args.debug:
                    remaining = list(blocks_to_execute.keys())
                    num_blocks_ran = 0

                    while remaining:
                        to_delete = []
                        for name in remaining:
                            block = blocks_to_execute[name]
                            if self.ready_to_run(block):
                                execute_block_debug(block, name, self.instances,
                                         self.args.cache_time, num_blocks_ran, self.num_blocks_executed)
                                to_delete.append(name)
                                block.complete = True
                                num_blocks_ran += 1

                        for name in to_delete:
                            remaining.remove(name)

                        sleep(.1)
                else:
                    with Pool(processes=nprocs) as pool:
                        results = dict()
                        remaining = list(blocks_to_execute.keys())
                        num_blocks_ran = 0
                        num_exceptions = 0
                        while remaining or results:
                            to_delete = []
                            for name in remaining:
                                block = blocks_to_execute[name]
                                if self.ready_to_run(block):
                                    results[name] = pool.apply_async(execute_block, 
                                            (block, name, self.instances, self.args.cache_time, num_blocks_ran, self.num_blocks_executed))
                                    to_delete.append(name)
                                    num_blocks_ran += 1

                            for name in to_delete:
                                remaining.remove(name)

                            to_delete = []
                            for name, result in results.items():
                                if result.ready():
                                    try:
                                        result.get()
                                    except Exception as err:
                                        num_exceptions += 1
                                        logging.error(err)

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

                        if blocks_to_execute:
                            self.notifications.append(partial(notify.send_finish_message,
                                                        filename=self.filename, 
                                                        njobs=len(blocks_to_execute),
                                                        time=time() - t_start,
                                                        num_exceptions=num_exceptions))
                        
                        if USE_SERVER:
                            t.join()
                            self.pipe.close()

                        display.cached_function_summary(self.num_blocks_executed, num_exceptions)

        numpipe._pbars.set_njobs(1)
        numpipe._pbars.reset()
        numpipe._pbars.auto_serial = True
        

        ### At-end functions
        if self.at_end_functions and not self.args.no_at_end:
            display.at_end_message()

            for func in self.at_end_functions.values():
                func()
        else:
            if self.args.notify:
                self.send_notifications(check_idle=False, idle=True)

    def ready_to_run(self, block):
        if block.dependencies is None:
            return True
        
        for D in block.dependencies:
            if not self.blocks[D].complete:
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
    def add(self, _func, _instance_name=None, **kwargs):
        """
        Add an instance (a function with specified kwargs)
        """
        if _instance_name is None:
            _instance_name = ''

        def get_new_name(name, addons):
            if name and not addons:
                return name
            elif name and addons:
                return name + '-' + '-'.join(addons)
            else:
                return '-'.join(addons)

        kwarg_params = dict()
        kwarg_params_outer = dict()
        for key, val in kwargs.items():
            if isinstance(val, numpipe.parameter):
                if val.outer:
                    kwarg_params_outer[key] = val
                else:
                    kwarg_params[key] = val

        if kwarg_params_outer and kwarg_params:
            args1 = [p.arg for p in kwarg_params_outer.values()]
            labels1 = itertools.product(*[p.labels for p in kwarg_params_outer.values()])

            for vals1 in itertools.product(*args1):
                post1 = list(filter(lambda x: x, next(labels1)))

                args2 = [p.arg for p in kwarg_params.values()]
                labels2 = zip(*[p.labels for p in kwarg_params.values()])
                for vals2 in zip(*args2):
                    new_kwargs = copy(kwargs)
                    replace = dict(zip(kwarg_params.keys(), vals2))
                    replace.update(zip(kwarg_params_outer.keys(), vals1))
                    new_kwargs.update(replace)

                    post2 = post1 + list(filter(lambda x: x, next(labels2)))
                    new_name = get_new_name(_instance_name, post2)

                    self.add(_func, new_name, **new_kwargs)

            return #TODO: return a block_collection that can call depends() on all or be indexed
        elif kwarg_params_outer:
            args = [p.arg for p in kwarg_params_outer.values()]
            labels = itertools.product(*[p.labels for p in kwarg_params_outer.values()])
            for vals in itertools.product(*args):
                new_kwargs = copy(kwargs)
                replace = dict(zip(kwarg_params_outer.keys(), vals))
                new_kwargs.update(replace)
                
                post = filter(lambda x: x, next(labels))
                new_name = get_new_name(_instance_name, post)
                self.add(_func, new_name, **new_kwargs)

            return #TODO: return a block_collection that can call depends() on all or be indexed
        elif kwarg_params:
            args = [p.arg for p in kwarg_params.values()]
            labels = zip(*[p.labels for p in kwarg_params.values()])
            for vals in zip(*args):
                new_kwargs = copy(kwargs)
                replace = dict(zip(kwarg_params.keys(), vals))
                new_kwargs.update(replace)

                post = filter(lambda x: x, next(labels))
                new_name = get_new_name(_instance_name, post)
                self.add(_func, new_name, **new_kwargs)

            return #TODO: return a block_collection that can call depends() on all or be indexed

        if _instance_name in self.instance_counts[_func.__name__]:
            self.instance_counts[_func.__name__][_instance_name] += 1
        else:
            self.instance_counts[_func.__name__][_instance_name] = 0

        count = self.instance_counts[_func.__name__][_instance_name]

        if _instance_name:
            block_name = f'{_func.__name__}-{_instance_name}-{count}'
        else:
            block_name = f'{_func.__name__}-{count}'

        filepath = f'{self.dirpath}/{self.filename}-{block_name}.h5'

        self.blocks[block_name] = block(
                          deferred_function(_func, kwargs=kwargs, num_iterations=None),
                          target(filepath),
                          dependencies=self.instance_dependency.get(_func.__name__, None))
        self.instances[_func.__name__].append(block_name)

        return self.blocks[block_name]

    def fix_block_names(self):
        for func_name, D in self.instance_counts.items():
            for name, counts in D.items():
                if counts == 0:
                    old_block_name = f'{func_name}-{name}-0' if name else f'{func_name}-0'
                    new_block_name = f'{func_name}-{name}' if name else f'{func_name}'
                    self.blocks[new_block_name] = self.blocks[old_block_name]
                    self.blocks.pop(old_block_name)

                    index = self.instances[func_name].index(old_block_name)
                    self.instances[func_name][index] = new_block_name

                    filepath = f'{self.dirpath}/{self.filename}-{new_block_name}.h5'
                    self.blocks[new_block_name].target.filepath = filepath

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
            self.instance_counts[func.__name__] = dict()
            if depends is not None:
                if isinstance(depends, str) or not isinstance(depends, Iterable):
                    self.instance_dependency[func.__name__] = [depends]
                else:
                    self.instance_dependency[func.__name__] = depends

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

            send_figures = True
            try:
                ret = func()
                if isinstance(ret, types.GeneratorType):
                    for anim in ret:
                        self.add_animation(anim)
                elif ret is not None:
                    self.add_animation(ret)
            except Exception as err:
                traceback.print_exception(type(err), err, err.__traceback__)
                if not plt.get_fignums():
                    plt.figure()
                    send_figures = False
                    message = '`@plots` threw an error before any figures were created'
                else:
                    message = '`@plots` threw an error, some images may not be sent'

                self.notifications.append(partial(notify.send_message,
                                message=message))

            animated_figs = self.animations.keys()

            if self.args.save_figs != '' or self.args.save != '':
                arg = self.args.save_figs if self.args.save_figs != '' else self.args.save
                for ext in self.args.save_format:
                    mpl_tools.save_figures(self.filename, arg,
                                           self.args.figures, exempt=animated_figs,
                                           ext=ext)

            if self.args.save_anims != '' or self.args.save != '':
                arg = self.args.save_anims if self.args.save_anims != '' else self.args.save
                for fignum, anim in self.animations.items():
                    filename = f'{self.filename}_vid{fignum}.mp4'
                    filepath = mpl_tools.get_filepath(filename, arg)
                    mpl_tools.save_animation(anim, filepath)

            if (self.num_blocks_executed > 0 or self.args.notify) and send_figures:
                self.notifications.append(partial(notify.send_images,
                                            filename=self.filename, exempt=animated_figs))
                self.notifications.append(partial(notify.send_videos,
                                            anims=self.animations.values()))

            if self.args.notify:
                self.send_notifications(check_idle=False, idle=True)
            else:
                self.send_notifications()

            plt.show = show_copy
            if self.args.figures is not None:
                [plt.close(plt.figure(i)) for i in plt.get_fignums() if i not in self.args.figures]
            plt.show()

        self.at_end_functions[func.__name__] = deferred_function(wrap)

        return wrap

    def add_animation(self, anim):
        """add an animation to the saved animations"""

        def add_single_animation(anim):
            key = anim._fig.number
            if key in self.animations:
                self.animations[key].append(anim)
            else:
                self.animations[key] = [anim]

        if isinstance(anim, Iterable): 
            for a in anim:
                add_single_animation(a)
        else:
            add_single_animation(anim)

    def send_notifications(self, **kwargs):
        t = threading.Thread(target=partial(notify.send_notifications, 
                                       notifications=self.notifications,
                                       delay=self.args.notify_delay,
                                       **kwargs))
        t.start()

    def shared(self, class_type):
        """decorator to add a class for shared variables"""
        return class_type

    def display_functions(self):
        display.display_message(self.blocks, self.instances, self.at_end_functions)

    def _clean(self, filepaths):
        """clean a set of filepaths

           Argumnets: 
               filepaths      list of filepaths to hdf5 files
        """
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

    def resolve_dependencies_down(self, blocks):
        for label, block in self.blocks.items():
            for D in copy(block.dependencies):
                all_deps = self.instances.get(D)
                if all_deps is not None:
                    block.dependencies.remove(D)
                    block.dependencies.extend(all_deps)

        for label, block in self.blocks.items():
            for D in block.dependencies:
                if label not in self.blocks[D].children:
                    self.blocks[D].children.append(label)

        if self.args.rerun is not None and len(self.args.rerun) != 0:
            # DOWN the tree
            block_dependencies = blocks
            if not self.args.no_deps:
                while block_dependencies:
                    new_blocks = dict()
                    for label, block in block_dependencies.items():
                        for child in block.children:
                            new_blocks[child] = self.blocks[child]

                    blocks.update(new_blocks)
                    block_dependencies = new_blocks


    def resolve_dependencies_up(self, blocks):
        # UP the tree
        block_dependencies = blocks
        while block_dependencies:
            new_blocks = dict()
            for label, block in block_dependencies.items():
                for dependency in block.dependencies:
                    if not self.blocks[dependency].target.exists():
                        new_blocks[dependency] = self.blocks[dependency]
                    else:
                        self.blocks[dependency].complete = True

            blocks.update(new_blocks)
            block_dependencies = new_blocks

    def delete(self):
        """
        delete target data
        """
        targets_to_delete = []

        if len(self.args.delete) == 0:
            targets_to_delete.extend([block.target for block in self.blocks.values()])
        else:
            for name in self.args.delete:
                labels = self.get_labels(name)
                targets_to_delete.extend([self.blocks[label].target for label in labels])

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

    def _init_logging(self):
        self.logfile = pathlib.Path(self.dirpath) / f'{self.filename}.log'
        logging.basicConfig(filename=self.logfile, filemode='w', level=logging.INFO,
                            format='%(levelname)s: %(message)s')
        logging.captureWarnings(True)
