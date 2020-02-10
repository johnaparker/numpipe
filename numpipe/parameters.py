import numpy as np
from numpipe.utility import flatten_along

class parameter():
    def __init__(self, arg, axis=None, gather=False, outer=False, labels=None):
        """a parameter for input to the scheduler.add function

        Arguments:
            arg       iterable / numpy array for values of the parameter
            axis      axis / axes over which to apply the parameter (default: all)
            gather    whether to gather results together at the end (default: False)
            outer     whether to perform an outer product over other parameters (default: False)
            labels    label for each argument to be added to the filename (default: none)
        """
        self.arg = flatten_along(np.asarray(arg), axis)
        self.axis = axis
        self.gather = gather
        self.outer = outer
        self.labels = labels
        if self.labels is None:
            self.labels = ['']*len(self.arg)
        else:
            self.labels = [str(l) for l in labels]

def gather(params, axis=None, outer=False):
    """a collection of parameters to be gathered together at the end

    Arguments:
        param     dictionary of {parameter_name: value}
        axis      axis / axes over which to gather (default: all)
        outer     whether to perform an outer product over the parameters (default: False)
    """
    all_params = dict()
    for name, val in params.items():
        all_params[name] = parameter(val, axis=axis, outer=outer, gather=True)

    return all_params

def outer(params, axis=None, gather=False):
    """a collection of parameters to be gathered together in an outer product

    Arguments:
        param     dictionary of {parameter_name: value}
        axis      axis / axes over which to gather (default: all)
        gather    whether to gather results together at the end (default: False)
    """
    all_params = dict()
    for name, val in params.items():
        all_params[name] = parameter(val, axis=axis, gather=gather, outer=True)

    return all_params
