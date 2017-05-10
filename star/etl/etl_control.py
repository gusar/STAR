import logging
import pandas as pd

from star.utils import log
from star.db.connector import DBConnector
from star.utils.arg_parser import ArgParser
from star.utils.config_utils import StarConfig
from star.config.config_name_map import *
from star.utils.pandas_utils import filter_unwanted_columns, extract_urls
from star.config.json_data_columns import *


DEFAULT_BATCH_SIZE = 100000


class ETLControl(object):
    def __init__(self, db_dict, batch_limit=DEFAULT_BATCH_SIZE):
        """
        :param db_dict: dict of staging, archive, clean connectors.
        """
        self.batch_limit = batch_limit
        self.staging_con = db_dict['staging']
        self.clean_con = db_dict['clean']
        self.archive_con = db_dict['archive']

    def clean_batch(self):
        batch_df = self.load_staging()
        self.write_to_archive(batch_df)
        batch_df = filter_unwanted_columns(batch_df, WANTED_COLUMNS)
        batch_df = extract_urls(batch_df)

        print(1)

    def load_staging(self):
        return self.staging_con.find(None, self.batch_limit)

    def write_to_archive(self, df):

        df_ids = df[ID_FIELD_DF].tolist()
        matching_ids = self.archive_con.find_distinct_list(df_ids, ID_FIELD_DF)
        ids_not_archived = list(set(df_ids) - set(matching_ids))
        logging.info('Writing to ARCHIVE: ' + str(len(ids_not_archived)))
        if len(ids_not_archived) > 0:
            self.archive_con.insert_df(df[df[ID_FIELD_DF].isin(ids_not_archived)])


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


def parse_args():
    arg_parser = ArgParser()
    arg_parser.add_config_path(True)
    return arg_parser.parse()


def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _staging = config[DB_TYPE][RAW_STAGING]
    _clean = config[DB_TYPE][CLEAN_STAGING]
    _archive = config[DB_TYPE][RAW_ARCHIVE]
    return {'staging': DBConnector(DB_TYPE, _uri, _db, _staging),
            'clean': DBConnector(DB_TYPE, _uri, _db, _clean),
            'archive': DBConnector(DB_TYPE, _uri, _db, _archive)}


def main():
    try:
        log.setup_logger()
        logging.info('Cleaning raw STAGING to CLEAN')
        if __debug__:
            pd.set_option('display.width', 10000)
            pd.set_option('display.max_columns', 50)
            pd.set_option('display.expand_frame_repr', True)
            pd.set_option('display.max_colwidth', 1500)

        args = parse_args()
        config = get_config_values(args.config)

        db_connector_dict = create_db_connectors(config)
        etl = ETLControl(db_connector_dict, config[DB_TYPE][BATCH_SIZE])
        etl.clean_batch()

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
