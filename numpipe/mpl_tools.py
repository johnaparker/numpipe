import matplotlib.pyplot as plt
import pathlib

def set_theme(name):
    if name == 'normal':
        return
    elif name == 'xkcd':
        plt.xkcd()
        return

    plt.style.use(name)

def save_figures(prefix, dirpath=None):
    """save all current matplotlib figures

    Arguments:
        prefix    prefix in filename  
        dirpath   path to directory where files are saved (default: current working directory)
    """
    if dirpath is None:
        dirpath = pathlib.Path.cwd()

    dirpath = pathlib.Path(dirpath).expanduser()
    for i in plt.get_fignums():
        filepath = dirpath / f'{prefix}_fig{i}.png'
        fig = plt.figure(i)
        fig.savefig(filepath)
