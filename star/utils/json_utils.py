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


def clean_json(json_obj):
    return dict(map(lambda (x, y): (x, y) if y is not None else (x, "None"),
                    json_obj.iteritems()))


def filter_by_columns(json_dict):
    return