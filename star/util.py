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


def flatten_json(dict_or_list):
    result_dict = {}

    def flatten(json_obj, name=''):
        if type(json_obj) is dict:
            [flatten(json_obj[a], name + a + '_') for a in json_obj]
        elif type(json_obj) is list:
            [flatten(a, name + str(i) + '_') for a, i in zip(json_obj, range(len(json_obj)))]
        else:
            try:
                a = json_obj
            except:
                a = json_obj.encode('ascii', 'ignore').decode('ascii')

            result_dict[name[:-1]] = a

    flatten(dict_or_list)
    return result_dict
