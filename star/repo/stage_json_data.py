import logging

import pandas as pd

from star import log
from star import utils
from star import repo
from star import db


class StockTwitStager:
    def __init__(self, repo_dir, db_name, collection, mongo_uri=None):
        self.repo_dir = repo_dir
        self.db_conn = None
        self.set_mongo_connection(db_name, collection, mongo_uri)

    def set_mongo_connection(self, db_name, collection, mongo_uri=None):
        mongo = db.Mongo(mongo_uri)
        mongo.set_db(db_name)
        mongo.set_collection(collection)
        self.db_conn = mongo

    def stage_stocktwits_files(self):
        logging.info('Partitioned repo: ' + self.repo_dir)

        sw_repo = repo.StockTwitsManager(self.repo_dir)
        stager = sw_repo.stager
        stager.next()
        return

    def stage_stocktwits_list(self):
        pass


def get_args():
    arg_parser = utils.ArgParser()
    arg_parser.add_partitioned_repo(True)
    arg_parser.add_mongo_uri()
    arg_parser.add_db_name(True)
    arg_parser.add_collection_name(True)
    return arg_parser.parse()


def main():
    log.setup_logger()
    pd.set_option('display.width', 1000)
    pd.set_option('display.expand_frame_repr', True)
    pd.set_option('display.max_colwidth', 1500)

    args = get_args()
    if args.mongo_uri:
        mongo_uri = args.mongo_uri

    stager = StockTwitStager(args.partitioned_repo,
                             args.db_name,
                             args.collection_name,
                             mongo_uri)


if __name__ == '__main__':
    main()
