import json
import pandas
import iso8601
import math
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
    """
    Extract URLs, combine results into one column of sets, append to original.
    :param df: DataFrame
    :return: DataFrame
    """
    url_extraction_regex = r"""(?P<url>https?://[^\s]+)"""
    urls_df = df.body.str.findall(url_extraction_regex)
    df['body_urls'] = urls_df.apply(lambda x: x if len(x) > 0 else None)
    return df


def extract_financial_symbols(df):
    """
    Extracts symbols starting with $ from a dataframe's body column.
    :param df: 
    :return: 
    """
    symbols_series = df.body.str.findall(r'(\$[a-zA-Z]\w+)')
    df['body_symbols'] = symbols_series.apply(lambda x: x if len(x) > 0 else None)
    return df


def parse_datetime_str_to_datetime64(df, col):
    """
    Parse and replace a column containing strings with datetimes
    :param df: DataFrame
    :param col: str: datetime col name
    :return: DataFrame
    """
    df[col] = df[col].apply(lambda x: iso8601.parse_date(x))
    return df
