# NumPipe
NumPipe is a Python software package that makes long-running tasks easier and faster by executing code in embarrassingly parallel and caching the output to HDF5 files.

## Features
* Combine computation and visualization code into single scripts. Only re-run computations on request
* Use the `yield` statement to return data over time that will be periodically cached to file
* Specify dependencies between cached functions
* Progress bars similar to `tqdm` that work in parallel to show the progress of running tasks
* An optional Telegram Messenger bot that can notify the user of completion and send Matplotlib figures and animations
* Command line arguments to re-run tasks and automatically save Matplotlib figures and animations
* Option to use the Slurm Workload Manager to automatically create and submit an sbatch file (used on many compute clusters)

## Installation
NumPipe can be installed with pip
```shell
pip install numpipe
```

## Examples

### single task
```python
from numpipe import scheduler, pbar
from time import sleep
import matplotlib.pyplot as plt

job = scheduler()

@job.cache
def sim():
    for i in pbar(range(100)):
        sleep(.1)   # long running task...
        yield dict(i=i, x=i**2)

@job.plots
def vis():
    var = job.load(sim)
    plt.plot(var.i, var.x)

if __name__ == '__main__':
    job.run()
```

### parallel tasks
```python
from numpipe import scheduler, pbar
from time import sleep
import matplotlib.pyplot as plt

job = scheduler()

@job.cache
def sim(power):
    for i in pbar(range(100)):
        sleep(.1)   # long running task...
        yield dict(i=i, x=i**power)

@job.plots
def vis():
    for name, var in job.load(sim):
        plt.plot(var.i, var.x)

for i in range(3):
    job.add(sim, power=i)

if __name__ == '__main__':
    job.run()
```
### more examples
See the [examples folder](https://github.com/johnaparker/numpipe/tree/master/examples) for more usage examples

## Command line arguments
```
positional arguments:
  {display,clean,slurm}
    display             display available functions and descriptions
    clean               remove all h5files that are no longer cache functions
    slurm               run on a system with the Slurm Workload Manager

optional arguments:
  -h, --help            show this help message and exit
  -r [RERUN [RERUN ...]], --rerun [RERUN [RERUN ...]]
                        re-run specific cached functions by name
  -f, --force           force over-write any existing cached data
  -d [DELETE [DELETE ...]], --delete [DELETE [DELETE ...]]
                        delete specified cached data
  -e EXCLUDE [EXCLUDE ...], --exclude EXCLUDE [EXCLUDE ...]
                        exclude cached function from being re-run
  --at-end              only run at_end functions
  --no-at-end           don't run at_end functions
  -p [PROCESSES], --processes [PROCESSES]
                        number of processes to use in parallel execution (default: cpu_count)
  -ct CACHE_TIME, --cache_time CACHE_TIME
                        time (in seconds) until data cached data is flushed to file
  --no-deps             do not rerun functions that depend on other reran functions
  --mininterval MININTERVAL
                        time (in seconds) for progress bar mininterval argument
  --notify              send notifications without delay
  --notify-message NOTIFY_MESSAGE
                        send a custom message with other notifications
  --notify-delay NOTIFY_DELAY
                        time (in seconds) before notifications will be sent
  --theme THEME         matplotlib plot theme
  --figures FIGURES [FIGURES ...]
                        which figure numbers to display
  --save [SAVE]         save figures and animations
  --save-format SAVE_FORMAT [SAVE_FORMAT ...]
                        file format for figures
  --save-figs [SAVE_FIGS]
                        save figures
  --save-anims [SAVE_ANIMS]
                        save animations
  --debug               run in debug mode (single process)
```

## License
NumPipe is licensed under the terms of the MIT license.
