import matplotlib.pyplot as plt
import pathlib

def set_theme(name):
    if name == 'normal':
        return
    elif name == 'xkcd':
        plt.xkcd()
        return

    plt.style.use(name)

def save_figures(prefix, dirpath=None, figures=None):
    """save all current matplotlib figures

    Arguments:
        prefix    prefix in filename  
        dirpath   path to directory where files are saved (default: current working directory)
        figures   list of figure numbers to display (default: all)
    """
    if dirpath is None:
        dirpath = pathlib.Path.cwd()

    dirpath = pathlib.Path(dirpath).expanduser()

    all_fignums = plt.get_fignums()
    if figures is None:
        fignums = all_fignums
    else:
        fignums = figures
        for num in fignums:
            if num not in all_fignums:
                raise IndexError(f'Failed to save Figure {num}: figure does not exist')


    for i in fignums:
        filepath = dirpath / f'{prefix}_fig{i}.png'
        fig = plt.figure(i)
        fig.savefig(filepath)
