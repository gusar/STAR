import logging


def setup_logger():
    logging.basicConfig(format='%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s',
                        level=logging.DEBUG)
    logging.info('Begin')
