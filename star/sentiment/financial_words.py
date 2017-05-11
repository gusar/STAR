import pandas as pd
from star.config.config_name_map import *
import logging

from star.config.json_data_columns import WANTED_COLUMNS_WORDS
from star.db.connector import DBConnector
from star.utils import log, ArgParser
from star.utils.config_utils import StarConfig
from star.utils.pandas_utils import filter_unwanted_columns


class WordList:
    def __init__(self):
        self.clean_con = None
        self.words_con = None
        self.word_list = None

    def file_to_db(self, db_dict, wordlist_path):
        self.clean_con = db_dict['clean']
        self.words_con = db_dict['words']

        words_df = pd.read_excel(wordlist_path)
        words_df = filter_unwanted_columns(words_df, WANTED_COLUMNS_WORDS)
        # words_df['sentiment'] = words_df.apply(lambda row: row.Negative)
        # words_df['Negative'] = words_df['Negative'] != 0
        # words_df['Positive'] = words_df['Positive'] != 0
        # words_df['Uncertainty'] = words_df['Uncertainty'] != 0
        # words_df['sentiment'] = words_df.any(1, True)
        # words_df = words_df[words_df.sentiment == True]
        # del(words_df['Negative'])
        # del (words_df['Positive'])
        # del (words_df['Uncertainty'])

        words_df = words_df[(words_df.Negative != 0) | (words_df.Positive != 0) | (words_df.Uncertainty != 0)]


        print(1)


def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _clean = config[DB_TYPE][CLEAN_STAGING]
    _words = config[DB_TYPE][SENTIMENT_WORDS]
    return {'words': DBConnector(DB_TYPE, _uri, _db, _words),
            'clean': DBConnector(DB_TYPE, _uri, _db, _clean)}


def parse_args():
    arg_parser = ArgParser()
    arg_parser.add_config_path(True)
    arg_parser.add_path(True)
    return arg_parser.parse()


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


def main():
    try:
        log.setup_logger()
        logging.info('Loading financial words')
        if __debug__:
            pd.set_option('compute.use_bottleneck', True)
            pd.set_option('compute.use_numexpr', True)
            pd.set_option('display.width', 10000)
            pd.set_option('display.max_columns', 50)
            pd.set_option('display.expand_frame_repr', True)
            pd.set_option('display.max_colwidth', 1500)

        args = parse_args()
        config = get_config_values(args.config)

        db_connector_dict = create_db_connectors(config)
        WordList().file_to_db(db_connector_dict, args.path)

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
