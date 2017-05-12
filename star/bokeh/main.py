import pandas as pd
import iso8601

from bokeh.io import curdoc
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import PreText, Select
from bokeh.plotting import figure

from star.bokeh.load_data import load_sentiment_data
from star.config.config_name_map import *
from star.config.json_data_columns import DATE_FIELD
from star.db.connector import DBConnector
from star.utils.config_utils import StarConfig



def create_graph():
    pass


def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _sentiments = config[DB_TYPE][SENTIMENTS]
    return {'sentiments': DBConnector(DB_TYPE, _uri, _db, _sentiments)}


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


def main():
    try:
        if __debug__:
            pd.set_option('compute.use_bottleneck', True)
            pd.set_option('compute.use_numexpr', True)
            pd.set_option('display.width', 10000)
            pd.set_option('display.max_columns', 50)
            pd.set_option('display.expand_frame_repr', True)
            pd.set_option('display.max_colwidth', 1500)

        config = get_config_values("/home/andy/PycharmProjects/STAR/star/config/star.yml")
        db_con = create_db_connectors(config)
        load_sentiment_data(['JNUG'], iso8601.parse_date('2014-12-01'), iso8601.parse_date('2015-01-01'), db_con)

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
