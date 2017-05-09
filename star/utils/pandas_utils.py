import json
import pandas
from bson import json_util


def mongo_to_df(mongo_cursor):
    sanitized = json.loads(json_util.dumps(mongo_cursor))
    normalized = pandas.io.json.json_normalize(sanitized)
    df = pandas.DataFrame(normalized)
    df.columns = [x.replace('.', '_') for x in df.columns]
    return df


def filter_unwanted_columns(df, wanted_cols):
    [df.pop(col_name) for col_name in df if col_name not in wanted_cols]
    return df


def extract_urls(df):
    url_extraction_regex = r"""(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))"""
    df.body.str.extract(url_extraction_regex)
    return df.apply(lambda x: set(x.values), axis=1)
