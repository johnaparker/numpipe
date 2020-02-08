import pathlib
import toml
import os

def get_config_path():
    """get the path to the configuration file"""
    path = pathlib.Path('~/.config/numpipe/numpipe.conf').expanduser()
    return path

def get_config():
    """get the configuration file contents as a dictionary"""
    path = get_config_path()
    config = toml.load(path)
    return config

def get_terminal_rows():
    rows = get_config()['tqdm']['max_rows']
    actual_rows, _ = os.popen('stty size', 'r').read().split()
    actual_rows = int(actual_rows) - 1
    rows = min(rows, actual_rows)

    return rows
