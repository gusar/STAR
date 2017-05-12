import pandas as pd
import iso8601

from star.config.config_name_map import *
from star.config.json_data_columns import DATE_FIELD
from star.db.connector import DBConnector
from star.utils.config_utils import StarConfig


from star.yahoo_finance.yql_tools import YahooFinance


def load_sentiment_data(tickers, from_date, to_date, db):
    tickers = ['JNUG']
    sentiments_con = db['sentiments']
    sentiments = sentiments_con.find_between_dates(from_date, to_date, DATE_FIELD)
    sentiments['Date'] = pd.to_datetime(sentiments['object_postedTime_$date'], unit='ms')
    del(sentiments['object_postedTime_$date'])
    sentiments['body_symbols'] = sentiments['body_symbols'].apply(lambda x: [y.replace('$', '') for y in x])
    sentiments['mask'] = sentiments['body_symbols'].apply(lambda x: True if set(x).intersection(tickers) else None)
    sentiments = sentiments[sentiments['mask'].notnull()]
    sentiments = sentiments.set_index(pd.DatetimeIndex(sentiments['Date']))
    sentiments = sentiments.groupby(pd.Grouper(key='Date', freq='1d')).agg(sum)
    sentiments.dropna().reset_index(drop=True)

    yf = YahooFinance(tickers, '2014-12-01', '2015-01-01')
    history = yf.request_historical()[tickers[0]]
    history['Date'] = pd.to_datetime(history['Date'])
    history['Close'] = history['Close'].apply(float)
    return sentiments, history


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
