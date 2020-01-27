"""
numpipe
=======
"""

from . import h5cache
from . import networking
from . import slurm
from . import utility
from . import fileio
from . import execution
from . import parser
from . import display

from .utility import once
from .numpipe import scheduler

from tqdm import tqdm as tq
_tqdm_mininterval = .1
def tqdm(*args, **kwargs):
    kw = dict(mininterval=_tqdm_mininterval)
    kw.update(kwargs)
    return tq(*args, **kw)

import tqdm as t
t.tqdm = tqdm
