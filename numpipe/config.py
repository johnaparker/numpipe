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

def get_terminal_rows_cols():
    rows, cols = os.popen('stty size', 'r').read().split()
    return rows - 1, cols - 1
