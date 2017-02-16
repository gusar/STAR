import argparse


class ArgParser:
    def __init__(self):
        self._arg_parser = argparse.ArgumentParser()

    def parse(self):
        known_args, unknown_args = self._arg_parser.parse_known_args()
        if unknown_args:
            return known_args, unknown_args
        return known_args

    def add_config_path(self, is_required=False):
        self._arg_parser.add_argument('-C', '--config',
                                      required=is_required,
                                      help='Full dir path to the yml config file.')

    def add_original_repo(self, is_required=False):
        self._arg_parser.add_argument('-or', '--original_repo',
                                      required=is_required,
                                      help='Full dir path to original large JSON files')

    def add_partitioned_repo(self, is_required=True):
        self._arg_parser.add_argument('-pr', '--partitioned_repo',
                                      required=is_required,
                                      help='Full dir path with partitioned JSON files')

    def add_split_size(self, is_required=False):
        self._arg_parser.add_argument('-ss', '--split_size',
                                      required=is_required,
                                      type=int,
                                      help='Number of lines per file')

    def add_mongo_uri(self, is_required=False):
        self._arg_parser.add_argument('-mu', '--mongo_uri',
                                      is_required=is_required,
                                      help='Connection URI for MongoDB')

    def add_db_name(self, is_required=False):
        self._arg_parser.add_argument('-db', '--db_name',
                                      is_required=is_required,
                                      help='Database name')

    def add_collection_name(self, is_required=False):
        self._arg_parser.add_argument('-col', '--collection_name',
                                      is_required=is_required,
                                      help='MongoDB collection name')
