"""
numpipe
=======
"""

from tqdm import tqdm as tq
_tqdm_mininterval = .1
def tqdm(*args, **kwargs):
    kw = dict(mininterval=_tqdm_mininterval, dynamic_ncols=True)
    kw.update(kwargs)
    return tq(*args, **kw)

import tqdm as t
t.tqdm = tqdm

from . import h5cache
from . import networking
from . import slurm
from . import utility
from . import fileio
from . import execution
from . import parser
from . import display
from . import notify
from . import config
from . import mpl_tools
from . import parameters

from .utility import once
from .numpipe import scheduler
from .parameters import parameter, gather, outer
