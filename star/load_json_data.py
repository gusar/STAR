import gzip
import json
import pandas as pd
import smart_open as so
from flatten_json import flatten_json
import logging

from util import list_files_by_ext_sorted


class StockTwitsLoader:

    def __init__(self, repo_path):
        self.repo_path = repo_path
        self.file_list = list_files_by_ext_sorted(repo_path)

    def load_stocktwits_df(self, json_file_path):
        logging.info('Reading file: ' + json_file_path)
        stocktwits_json_list = self._json_file_to_str_list(json_file_path)
        df = self._json_str_list_to_dataframe(stocktwits_json_list)
        logging.info(str(df.shape))
        return df

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
    def _json_str_list_to_dataframe(json_str_list):
        dataframe_row_list = [pd.DataFrame(flatten_json(json.loads(json_str)), index=[0])
                              for json_str in json_str_list]
        return pd.concat(dataframe_row_list)

    @staticmethod
    def _read_from_archive(file_path):
        gzip_reader = gzip.open(file_path, 'rb')
        stocktwits_json = gzip_reader.read()
        gzip_reader.close()
        return stocktwits_json
