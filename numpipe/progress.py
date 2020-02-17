from time import sleep
from random import random
from multiprocessing import Pool, Lock, Value, Array
from termcolor import colored
from time import time

def format_seconds(T):
    """format time T (in seconds) to a display string"""
    ret_fmt = '{mins}:{secs}'
    hrs = int(T // 3600)
    mins = int((T // 60) - 60*hrs)
    secs = int((T // 1) - 3600*hrs - 60*mins)

    ret = ret_fmt.format(mins=str(mins).zfill(2),
                       secs=str(secs).zfill(2))
    if hrs:
        ret = f'{hrs}:{ret}'

    return ret

class progress_bars:
    def __init__(self, njobs=1, mininterval=.1, character='#'):
        """a collection of progress bars that work with multiprocessing"""

        ### per-process variables
        self.pbar_fmt = '{desc}{percent}|{bars}| {count} [{t_ran}<{t_left}, {iters}]'
        self.pbar_kwargs = dict()
        self.pos = 0
        self.max_col = 119
        self.character = character  # '█'
        self.pbar_col = 50
        self.mininterval = mininterval
        self.pbar_kwargs['desc'] = ''

        ### global variables
        self.lock = Lock()
        self.pos_g = Value('i', 0, lock=False)
        self.pos_arr = Array('i', range(njobs), lock=False)

    def set_njobs(self, njobs):
        """set the number of jobs"""
        self.pos_arr = Array('i', range(njobs), lock=False)

    def set_desc(self, desc):
        """set the number of jobs"""
        self.pbar_kwargs['desc'] = f'{desc}: '

    def progress(self, it, mininterval=None, desc=None):
        """obtain a new progress bar from an iterable"""
        total = len(it)

        if mininterval is None:
            mininterval = self.mininterval

        if desc is not None:
            self.set_desc(disc)

        with self.lock:
            self.pos = self.pos_g.value
            self.pos_g.value += 1

            self._initialize_bar(total)

        ctime = time()
        start_time = time()

        for counter, val in enumerate(it):
            if time() - ctime > mininterval or counter == total-1:
                fraction = counter/(total-1)
                cols = int(fraction*self.pbar_col)

                time_passed = time() - start_time
                iters = counter/time_passed
                time_left = (total - 1 - counter)/iters
                iters_str = f'{iters:.2f}it/s' if iters > 1 else f'{1/iters:.2f}s/it'

                self.pbar_kwargs.update(dict(bars=(self.character*cols).ljust(self.pbar_col),
                                             percent=f'{fraction*100:.0f}%'.rjust(4),
                                             count=f'{counter+1}/{total}',
                                             t_ran=format_seconds(time_passed),
                                             iters=iters_str,
                                             t_left=format_seconds(time_left)))

                with self.lock:
                    if counter == total-1:
                        self.finish_bar()
                    else:
                        self._write_pbar_str()

                ctime = time()

            yield val

    def finish_bar(self):
        """set the bar status to complete"""
        self.pbar_kwargs['desc'] = colored(self.pbar_kwargs['desc'], color='green', attrs=['bold'])
        self._write_pbar_str(flush=False)
        self._move_bar()

    def fail_bar(self):
        """set the bar status to failure"""
        with self.lock:
            self.pbar_kwargs['desc'] = colored(self.pbar_kwargs['desc'], color='red', attrs=['bold'])
            self._write_pbar_str(flush=False)
            self._move_bar()

    def _move_bar(self):
        """move the pbar to cursor and move the cursor down"""
        pos = self.pos_arr[self.pos]

        self._clear_line(flush=False)
        self._write_pbar_str(pos=0, flush=False)
        print(flush=True)

        for i,val in enumerate(self.pos_arr):
            if val > pos:
                self.pos_arr[i] -= 1

    def _initialize_bar(self, total):
        """initialize and display the pbar"""
        self.pbar_kwargs.update(dict(bars=''.ljust(self.pbar_col),
                                     percent=f'0%'.rjust(4),
                                     count=f'(0/{total})',
                                     t_ran=format_seconds(0),
                                     iters='?it/s',
                                     t_left='?'))

        self._write_pbar_str()

    def _clear_line(self, flush=True):
        """clear the current line"""
        self._write_line(' '*200, flush=flush)

    def _write_pbar_str(self, pos=None, flush=True):
        """write the current pbar string"""
        pbar_str = self.pbar_fmt.format(**self.pbar_kwargs)
        self._write_line(pbar_str, pos=pos, flush=flush)

    def _write_line(self, line, pos=None, flush=True):
        """write a line at a given position"""
        if pos is None:
            pos = self.pos_arr[self.pos]

        print('\n'*pos, end='')
        print('\r', end='')
        print(line.ljust(self.max_col), end='')
        print('\033[F'*pos, end='')
        print('\033[1000C', end='', flush=flush)

