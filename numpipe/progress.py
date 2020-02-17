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
    def __init__(self, njobs=1, mininterval=100):
        """a collection of progress bars that work with multiprocessing"""

        ### per-process variables
        self.bar_fmt = '{percent}|{bars}| {count} [{t_ran}<{t_left}, {iters}]'
        self.pbar_str = ''
        self.pos = 0
        self.max_col = 119
        self.bar_symbol = '#'   #'█'
        self.pbar_col = 50
        self.mininterval = mininterval
        self.desc = None

        ### global variables
        self.lock = Lock()
        self.pos_g = Value('i', 0, lock=False)
        self.pos_arr = Array('i', range(njobs), lock=False)

    def set_njobs(self, njobs):
        """set the number of jobs"""
        self.pos_arr = Array('i', range(njobs), lock=False)

    def set_desc(self, desc):
        """set the number of jobs"""
        self.desc = desc

    def progress(self, it, mininterval=None, desc=None):
        """obtain a new progress bar from an iterable"""
        total = len(it)

        if mininterval is None:
            mininterval = self.mininterval

        bar_fmt = self.bar_fmt
        if desc is not None:
            bar_fmt = f'{desc}: {self.bar_fmt}'
        elif self.desc is not None:
            bar_fmt = f'{self.desc}: {self.bar_fmt}'

        with self.lock:
            self.pos = self.pos_g.value
            self.pos_g.value += 1

            self._initialize_bar(total, bar_fmt)

        ctime = time()
        start_time = time()

        for counter, val in enumerate(it):
            if time() - ctime > mininterval/1e3 or counter == total-1:
                fraction = counter/(total-1)
                cols = int(fraction*self.pbar_col)

                time_passed = time() - start_time
                iters = counter/time_passed
                time_left = (total - 1 - counter)/iters
                iters_str = f'{iters:.2f}it/s' if iters > 1 else f'{1/iters:.2f}s/it'

                self.pbar_str = bar_fmt.format(bars=(self.bar_symbol*cols).ljust(self.pbar_col),
                                               percent=f'{fraction*100:.0f}%'.rjust(4),
                                               count=f'({counter+1}/{total})',
                                               t_ran=format_seconds(time_passed),
                                               iters=iters_str,
                                               t_left=format_seconds(time_left))

                with self.lock:
                    if counter == total-1:
                        self.finish_bar()
                    else:
                        self._write_pbar_str()

                ctime = time()

            yield val

    def finish_bar(self):
        """set the bar status to complete"""
        # self.pbar_str = colored(('PASSED ' + self.pbar_str).ljust(self.max_col), color='green')
        self.pbar_str = (colored('PASSED ', color='green') + self.pbar_str).ljust(self.max_col)
        self._write_pbar_str(flush=False)
        self._move_bar()

    def fail_bar(self):
        """set the bar status to failure"""
        with self.lock:
            self.pbar_str = colored(('FAILED ' + self.pbar_str).ljust(self.max_col), color='red')
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

    def _initialize_bar(self, total, bar_fmt=None):
        """initialize and display the pbar"""
        pbar = bar_fmt.format(bars=''.ljust(self.pbar_col),
                              percent=f'0%'.rjust(4),
                              count=f'(0/{total})',
                              t_ran=format_seconds(0),
                              iters='?it/s',
                              t_left='?')

        self.pbar_str = pbar
        self._write_pbar_str()

    def _clear_line(self, flush=True):
        """clear the current line"""
        self._write_line(' '*200, flush=flush)

    def _write_pbar_str(self, pos=None, flush=True):
        """write the current pbar string"""
        self._write_line(self.pbar_str, pos=pos, flush=flush)

    def _write_line(self, line, pos=None, flush=True):
        """write a line at a given position"""
        if pos is None:
            pos = self.pos_arr[self.pos]

        print('\n'*pos, end='')
        print('\r', end='')
        print(line.ljust(self.max_col), end='')
        print('\033[F'*pos, end='')
        print('\033[1000C', end='', flush=flush)

