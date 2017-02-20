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


def get_full_file_path(folder_name, file_name):
    root_dir = os.path.dirname(os.getcwd())
    target_dir = os.path.join(root_dir, folder_name)
    target_file_path = os.path.join(target_dir, file_name)

    if os.path.exists(target_file_path) is True:
        return target_file_path
    else:
        raise IOError
