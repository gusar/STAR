import yaml

from star.utils.exceptions import NoneString


class StarConfig(object):

    def __init__(self, config_path, config_type='yaml'):
        """
        Config parser for STAR project
        :param config_path: str
        :param config_type: str, default: 'yaml'
        """
        if config_path is None:
            raise NoneString

        self._type = config_type
        self._path = config_path
        self.parsed_content = None

    def parse(self):
        self.parsed_content = self.type_map[self._type]()
        return self.parsed_content

    @property
    def type_map(self):
        return {
            'yaml': self.parse_yaml_config
        }

    def parse_yaml_config(self):
        with open(self._path, 'r') as stream:
            try:
                return yaml.load(stream)
            except yaml.YAMLError as e:
                raise e
