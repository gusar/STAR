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


def flatten_json(list_or_dict):

    def flatten(json_obj, name=''):
    	obj_type = type(json_obj)
    	
        if obj_type is dict:
        	map(lambda x: flatten(json_obj[x], name + x + '_'), json_obj)
                
        elif obj_type is list:
            map(lambda x, i: flatten(x, name + str(i) + '_'), json_obj, len(json_obj))
                
        else:
            try:
                str_obj = str(json_obj)
            except:
                str_obj = json_obj.encode('ascii', 'ignore').decode('ascii')
            
            out[str(name[:-1])] = str_obj

    return flatten(list_or_dict)