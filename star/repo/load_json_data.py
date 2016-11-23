import gzip
import json
import logging

import pandas as pd
import smart_open as so
from pandas.io import json as pjson

from star import utils
from star.config import json_data_columns


def get_args():
    arg_parser = utils.ArgParser()
    arg_parser.add_partitioned_repo(True)
    return arg_parser.parse()


def load_stocktwits_files():
    args = get_args()
    logging.info('Partitioned repo: ' + args.partitioned_repo)

    sw_repo = StockTwitsManager(args.partitioned_repo)
    return sw_repo.loader


class StockTwitsManager:
    def __init__(self, repo_path):
        self.repo_path = repo_path
        self.file_list = utils.list_files_by_ext_sorted(repo_path, 'json', 'gz')
        self.db_collection = None

    def load_stocktwits_list(self, json_file_path):
        logging.info('Reading file: ' + json_file_path)
        stocktwits_json_list = self._json_file_to_str_list(json_file_path)
        tweets_list = self._normalize_json_str_list(stocktwits_json_list)
        logging.info(str(len(tweets_list)))
        return tweets_list

    def load_stocktwits_df(self, json_file_path):
        logging.info('Reading file: ' + json_file_path)
        stocktwits_json_list = self._json_file_to_str_list(json_file_path)
        df = self._json_str_list_to_dataframe(stocktwits_json_list)
        # df.to_csv("/home/andy/" + json_file_path.split('/')[-1] + ".csv", encoding='utf-8')
        logging.info(str(df.shape))
        return df

    def list_to_mongodb(self):
        pass

    def set_mongodb_collection(self, monogodb_collection):
        self.db_collection = monogodb_collection

    @property
    def stager(self):
        return self._stocktwits_stager_generator()

    def _stocktwits_stager_generator(self):
        for sw_file in self.file_list:
            yield self.load_stocktwits_list(sw_file)

    @property
    def loader(self):
        return self._stocktwits_loader_generator()

    def _stocktwits_loader_generator(self):
        for sw_file in self.file_list:
            yield self.load_stocktwits_df(sw_file)

    @staticmethod
    def _json_file_to_str_list(file_path):
        with so.smart_open(file_path) as fin:
            json_str_list = fin.readlines()
            fin.close()
        return json_str_list

    @staticmethod
    def _normalize_json_str_list(json_str_list):
        def json_to_norm_dict(s, c):
            logging.info(str(c))
            normalized_dict = pjson.nested_to_record(json.loads(s))
            return {key.replace('.', '_'): value for key, value in normalized_dict.iteritems()
                    if key in json_data_columns.WANTED_COLUMNS}

        return [json_to_norm_dict(json_str, count)
                for json_str, count in zip(json_str_list, range(len(json_str_list)))]

    @staticmethod
    def _json_str_list_to_dataframe(json_str_list):
        def json_to_df(s, c):
            logging.info(str(c))
            return pjson.json_normalize(json.loads(s))

        dataframe_row_list = [json_to_df(json_str, count)
                              for json_str, count in zip(json_str_list, range(len(json_str_list)))]
        return pd.concat(dataframe_row_list)

    @staticmethod
    def _read_from_archive(file_path):
        gzip_reader = gzip.open(file_path, 'rb')
        stocktwits_json = gzip_reader.read()
        gzip_reader.close()
        return stocktwits_json
