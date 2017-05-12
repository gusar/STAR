import logging
import pandas as pd

from star.etl.etl_control import ETLControl
from star.sentiment.financial_words import WordList
from star.utils import log
from star.db.connector import DBConnector
from star.utils.arg_parser import ArgParser
from star.utils.config_utils import StarConfig
from star.config.config_name_map import *


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


def parse_args():
    arg_parser = ArgParser()
    arg_parser.add_config_path(False)
    arg_parser.new_word_list(False)
    arg_parser.add_clean(False)
    arg_parser.add_analyse(False)
    return arg_parser.parse()


def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _staging = config[DB_TYPE][RAW_STAGING]
    _clean = config[DB_TYPE][CLEAN_STAGING]
    _archive = config[DB_TYPE][RAW_ARCHIVE]
    _words = config[DB_TYPE][SENTIMENT_WORDS]
    _sentiments = config[DB_TYPE][SENTIMENTS]
    return {'staging': DBConnector(DB_TYPE, _uri, _db, _staging),
            'clean': DBConnector(DB_TYPE, _uri, _db, _clean),
            'archive': DBConnector(DB_TYPE, _uri, _db, _archive),
            'words': DBConnector(DB_TYPE, _uri, _db, _words),
            'sentiments': DBConnector(DB_TYPE, _uri, _db, _sentiments)}


def main():
    try:
        log.setup_logger()
        logging.info('Cleaning raw STAGING to CLEAN')
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

        if args.word_list is not None:
            wl = WordList(db_connector_dict)
            wl.file_to_db(args.path)

        if args.clean:
            etl = ETLControl(db_connector_dict, config[DB_TYPE][BATCH_SIZE])
            etl.clean_batch()

        if args.analyse:
            wl = WordList(db_connector_dict)
            wl.determine_sentiments(config[DB_TYPE][BATCH_SIZE])

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
