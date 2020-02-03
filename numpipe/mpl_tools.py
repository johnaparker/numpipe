import matplotlib.pyplot as plt
import pathlib
from tqdm import tqdm

def set_theme(name):
    if name == 'normal':
        return
    elif name == 'xkcd':
        plt.xkcd()
        return

    plt.style.use(name)

def get_filepath(filename, dirpath=None):
    if dirpath is None:
        dirpath = pathlib.Path.cwd()

    dirpath = pathlib.Path(dirpath).expanduser()
    return dirpath / filename

def save_figures(prefix, dirpath=None, figures=None, exempt=None):
    """save all current matplotlib figures

    Arguments:
        prefix    prefix in filename  
        dirpath   path to directory where files are saved (default: current working directory)
        figures   list of figure numbers to display (default: all)
        exempt    list of figure numbers to not display, overriding figures argument (default: none)
    """
    all_fignums = plt.get_fignums()
    if figures is None:
        fignums = all_fignums
    else:
        fignums = figures
        for num in fignums:
            if num not in all_fignums:
                raise IndexError(f'Failed to save Figure {num}: figure does not exist')

    if exempt is not None:
        fignums = set(fignums) - set(exempt)

    for i in fignums:
        filepath = get_filepath(f'{prefix}_fig{i}.png', dirpath)
        fig = plt.figure(i)
        fig.savefig(filepath)

def save_animation(animation, filename, *args, **kwargs):
    """A wrapper for anim.save(...) that shows the progress of the saving

            animation   animation object, or list of animation objects
            filename    file output name
            *args       additional arguments to pass to anim.save
            **kwargs    additional keyword arguments to pass to anim.save
    """
    if isinstance(animation, list):
        anim = animation[0]
        extra_anim = animation[1:]
        kwargs.update({'extra_anim': extra_anim})
    else:
        anim = animation

    filepath = pathlib.Path(filename).resolve()

    progress = tqdm(total = anim.save_count+1, ascii=True, desc="Saving video '{}'".format(filepath.name))
    store_func = anim._func
    def wrapper(*args):
        progress.update()
        return store_func(*args)
    anim._func = wrapper
    anim.save(filepath, *args, **kwargs)
    anim._func = store_func
    progress.close()
