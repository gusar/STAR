from star.config import config_name_map as config_names
from star.db.mongo import Mongo
from star.utils import log
from star.utils.arg_parser import ArgParser
from star.utils.config_utils import StarConfig


class ETLControl(object):
    def __init__(self, db_connection):
        pass

    def clean_msg_by_month():
        pass


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


def parse_args():
    arg_parser = ArgParser()
    arg_parser.add_config_path(True)
    return arg_parser.parse()


def main():
    log.setup_logger()
    args = parse_args()
    config = get_config_values(args.config)
    mongo = Mongo(mongodb_uri=config[config_names.MONGO_URI],
                  db_name=config[config_names.MONGO_DB])
    etl = ETLControl(mongo)


if __name__ == '__main__':
    main()
