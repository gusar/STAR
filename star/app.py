import logging

from star import repo


def setup_logger():
    logging.basicConfig(format='%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s',
                        level=logging.DEBUG)
    logging.info('Begin')


def load_stocktwits_data():
    loader = repo.load_stocktwits_files()
    df = loader.next()
    return


def main():
    setup_logger()

    # star.split_stocktwits_files()
    sw_loader = load_stocktwits_data()
    logging.info('End')


if __name__ == '__main__':
    main()
