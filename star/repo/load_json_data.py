import gzip
import json
import logging

import pandas as pd
import smart_open as so

from star import utils


def get_args():
    arg_parser = utils.ArgParser()
    arg_parser.add_partitioned_repo(True)
    return arg_parser.parse()


def load_stocktwits_files():
    args = get_args()
    logging.info('Partitioned repo: ' + args.partitioned_repo)

    sw_repo = StockTwitsLoader(args.partitioned_repo)
    return sw_repo.loader


class StockTwitsLoader:
    def __init__(self, repo_path):
        self.repo_path = repo_path
        self.file_list = utils.list_files_by_ext_sorted(repo_path, 'json', 'gz')

    def load_stocktwits_df(self, json_file_path):
        logging.info('Reading file: ' + json_file_path)
        stocktwits_json_list = self._json_file_to_str_list(json_file_path)
        df = self._json_str_list_to_dataframe(stocktwits_json_list)
        df.to_csv("/home/andy/" + json_file_path.split('.')[-4:-2] + ".csv")
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
        dataframe_row_list = [pd.io.json.json_normalize(json.loads(json_str)) for json_str in json_str_list]
        print('done')
        return pd.concat(dataframe_row_list)



    @staticmethod
    def _read_from_archive(file_path):
        gzip_reader = gzip.open(file_path, 'rb')
        stocktwits_json = gzip_reader.read()
        gzip_reader.close()
        return stocktwits_json
