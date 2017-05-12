import pandas as pd

from star.config.config_name_map import *
from star.db.connector import DBConnector
from star.utils.config_utils import StarConfig


# def load_sentiment_data(tickers, from_date, to_date, db_connector_dict):
#     pass
def load_sentiment_data(db):
    sentiments_con = db['sentiments']
    sentiments_con.find(None, 10000)


def convert_to_date():
    pass



def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _clean = config[DB_TYPE][CLEAN_STAGING]
    _words = config[DB_TYPE][SENTIMENT_WORDS]
    _sentiments = config[DB_TYPE][SENTIMENTS]
    return {'words': DBConnector(DB_TYPE, _uri, _db, _words),
            'clean': DBConnector(DB_TYPE, _uri, _db, _clean),
            'sentiments': DBConnector(DB_TYPE, _uri, _db, _sentiments)}


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
        db_connector_dict = create_db_connectors(config)
        # load_sentiment_data(tickers, from_date, to_date, db_connector_dict)
        load_sentiment_data(db_connector_dict)

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
