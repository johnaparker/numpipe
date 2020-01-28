import pathlib
import toml

def get_config_path():
    """get the path to the configuration file"""
    path = pathlib.Path('~/.config/numpipe/numpipe.conf').expanduser()
    return path

def get_config():
    """get the configuration file contents as a dictionary"""
    path = get_config_path()
    config = toml.load(path)
    return config
