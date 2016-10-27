import logging
import star.load_json_data
import star


def setup_logger():
    logging.basicConfig(format='%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s',
                        level=logging.DEBUG)
    logging.info('Begin')


def load_stocktwits_data(path):
    return star.load_json_data.load_stocktwits_df(path)


def main():
    setup_logger()

    star.split_stocktwits_files()

    # sw_df = load_stocktwits_data(parts_path)

    logging.info('End')


if __name__ == '__main__':
    main()
