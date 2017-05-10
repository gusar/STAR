import pandas as pd
from yahoo_finance import Share

import logging
from star.utils import log


class YahooFinance:
    def __init__(self, symbols_list, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.symbols_list = [x.replace('$', '') for x in symbols_list]
        self.history_dict = {}

    def request_historical(self):
        for symbol_str in self.symbols_list:
            logging.info('Getting index history: ' + symbol_str)
            symbol = self.set_symbol(symbol_str)
            df = pd.DataFrame(symbol.get_historical(self.start_date, self.end_date))
            self.history_dict[symbol_str] = df
        return self.history_dict

    @staticmethod
    def set_symbol(symbol):
        return Share(symbol)


def main():
    try:
        log.setup_logger()
        logging.info('Cleaning raw STAGING to CLEAN')
        if __debug__:
            pd.set_option('display.width', 10000)
            pd.set_option('display.max_columns', 50)
            pd.set_option('display.expand_frame_repr', True)
            pd.set_option('display.max_colwidth', 1500)

        yf = YahooFinance(['$GLD', '$JNUG'], '2014-12-01', '2015-1-31')
        yf.request_historical()

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
