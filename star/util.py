import os
from glob import glob


def list_files_by_ext_sorted(dir_path, ext, postfix=None):
    search_list = ['*', ext, postfix] if postfix else ['*', ext]
    return sorted(glob(os.path.join(dir_path, '.'.join(search_list))))


def safe_create_path(path):
    try:
        os.makedirs(path)
    except (IOError, OSError):
        pass
    return path
