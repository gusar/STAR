import logging
import pandas as pd

from star.utils import log
from star.db.connector import DBConnector
from star.utils.arg_parser import ArgParser
from star.utils.config_utils import StarConfig
from star.config.config_name_map import *
from star.utils.pandas_utils import extract_urls, parse_datetime_str_to_datetime64, extract_financial_symbols
from star.config.json_data_columns import *

DEFAULT_BATCH_SIZE = 10000


class ETLControl(object):
    def __init__(self, db_dict, batch_limit=DEFAULT_BATCH_SIZE):
        """
        :param db_dict: dict of staging, archive, clean connectors.
        """
        self.batch_limit = batch_limit
        self.staging_con = db_dict['staging']
        self.clean_con = db_dict['clean']
        self.archive_con = db_dict['archive']
        self.id_field_df = ID_FIELD_DF
        self.id_field = ID_FIELD

    def clean_batch(self):
        i = 1
        while True:
            logging.info('ETL batch: {}'.format(i))
            batch_df = self.load_staging()
            logging.info('Loaded: {}'.format(len(batch_df)))
            if len(batch_df) < 1:
                break
            original_ids = batch_df[ID_FIELD_DF].tolist()
            # self.write_to_db(batch_df, self.archive_con)
            batch_df = batch_df[WANTED_COLUMNS_STOCKTWITS]
            batch_df = extract_financial_symbols(batch_df)
            batch_df = batch_df[batch_df['body_symbols'].notnull()]
            batch_df = extract_urls(batch_df)
            batch_df = parse_datetime_str_to_datetime64(batch_df, DATE_FIELD)
            # del(batch_df[ID_FIELD_DF])
            self.write_to_db(batch_df, self.clean_con)
            self.delete_from_staging(original_ids)
            i += 1

    def load_staging(self):
        return self.staging_con.find(None, self.batch_limit)

    def write_to_db(self, df, con):
        if self.id_field_df in df.columns:
            df_ids = df[self.id_field_df].tolist()
            matching_ids = con.find_distinct_list(df_ids, self.id_field_df)
            ids_not_archived = list(set(df_ids) - set(matching_ids))
            logging.info('Writing to {}: {}'.format(con.collection_name, str(len(ids_not_archived))))
            if len(ids_not_archived) < 1:
                return con.insert_df(df[df[self.id_field_df].isin(ids_not_archived)])
        con.insert_df(df)

    def delete_from_staging(self, df_ids):
        removed_result = self.staging_con.delete_by_id_in(df_ids)
        if hasattr(removed_result, 'deleted_count'):
            logging.info('Deleted from STAGING: {}'.format(removed_result.deleted_count))
        else:
            logging.info('Deleted from STAGING: count not found')


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
