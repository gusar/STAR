import logging

import pandas as pd

from star import log
from star import utils
from star import repo


def get_args():
    arg_parser = utils.ArgParser()
    arg_parser.add_partitioned_repo(True)
    return arg_parser.parse()


def stage_stocktwits_files(repo_dir):
    logging.info('Partitioned repo: ' + repo_dir)

    sw_repo = repo.StockTwitsManager(repo_dir)
    stager = sw_repo.stager
    stager.next()
    return


def main():
    log.setup_logger()
    pd.set_option('display.width', 1000)
    pd.set_option('display.expand_frame_repr', True)
    pd.set_option('display.max_colwidth', 1500)

    args = get_args()
    stage_stocktwits_files(args.partitioned_repo)

if __name__ == '__main__':
    main()
