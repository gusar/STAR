import json
import pandas
from bson import json_util


def mongo_to_df(mongo_cursor):
    sanitized = json.loads(json_util.dumps(mongo_cursor))
    normalized = pandas.io.json.json_normalize(sanitized)
    df = pandas.DataFrame(normalized)
    df.columns = [x.replace('.', '_') for x in df.columns]
    return df
