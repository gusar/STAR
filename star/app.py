import logging

from star import repo
import pandas as pd
from repo import load_json_data


def setup_logger():
    logging.basicConfig(format='%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s',
                        level=logging.DEBUG)
    logging.info('Begin')


def load_stocktwits_data():
    loader = repo.load_stocktwits_files()
    df = loader.next()
    return


def stage_stocktwits_data():
    stager = load_json_data.stage_stocktwits_files()
    return


def main():
    setup_logger()
    pd.set_option('display.width', 1000)
    pd.set_option('display.expand_frame_repr', True)
    pd.set_option('display.max_colwidth', 1500)

    # star.split_stocktwits_files()
    # sw_loader = load_stocktwits_data()
    stage_stocktwits_data()
    logging.info('End')


if __name__ == '__main__':
    main()