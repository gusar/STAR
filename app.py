import logging
import star.load_json_data
import star.split_json_files


def setup_logger():
    logging.basicConfig(format='%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s',
                        level=logging.DEBUG)
    logging.info('Started')


def split_stocktwits_files():
    return star.split_json_files.split_compressed_files()


def load_stocktwits_data(path):
    return star.load_json_data.load_stocktwits_df(path)


def main():
    setup_logger()

    split_stocktwits_files()

    # parts_path = split_json_files.PARTS_DIR
    # sw_df = load_stocktwits_data(parts_path)

    logging.info('End')


if __name__ == '__main__':
    main()
