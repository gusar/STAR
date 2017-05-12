import pandas as pd
from yahoo_finance import Share

import logging


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
            df = pd.DataFrame(symbol.get_historical(self.start_date.strftime("%Y-%m-%d"), self.end_date.strftime("%Y-%m-%d")))
            self.history_dict[symbol_str] = df
        return self.history_dict

    @staticmethod
    def set_symbol(symbol):
        return Share(symbol)
