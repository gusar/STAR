import pandas as pd
import logging

from bs4 import BeautifulSoup
from nltk import tokenize, corpus

from star.config.config_name_map import *
from star.config.json_data_columns import ID_FIELD_DF
from star.db.connector import DBConnector
from star.utils import log, ArgParser
from star.utils.config_utils import StarConfig


DEFAULT_BATCH_SIZE = 10000


class WordList:
    def __init__(self, db_dict):
        self.clean_con = db_dict['clean']
        self.words_con = db_dict['words']
        self.sentiments_con = db_dict['sentiments']
        self.word_list = None
        self.batch_limit = DEFAULT_BATCH_SIZE

    def file_to_db(self, wordlist_path):

        words_df = pd.read_excel(wordlist_path)
        words_df = words_df[(words_df.Negative != 0)
                            | (words_df.Positive != 0)
                            | (words_df.Uncertainty != 0)]
        words_df['sentiment'] = words_df.apply(lambda x: sentiment(x[1], x[2]), axis=1)
        del(words_df['Negative'])
        del (words_df['Positive'])
        del (words_df['Uncertainty'])
        words_df.reset_index(drop=True, inplace=True)
        words_df['Word'] = words_df['Word'].str.lower()
        self.write_to_db(words_df)

    def write_to_db(self, df):
        df_words = df['Word'].tolist()
        matching_words = self.words_con.find_distinct_list(df_words, 'Word')
        ids_not_archived = list(set(df_words) - set(matching_words))
        logging.info('Writing to {}: {}'.format(self.words_con.collection_name, str(len(ids_not_archived))))
        if len(ids_not_archived) > 0:
            self.words_con.insert_df(df[df['Word'].isin(ids_not_archived)])

    def determine_sentiments(self, batch_limit):
        self.word_list = self.load_words()
        if batch_limit:
            self.batch_limit = batch_limit
        i = 1
        while True:
            logging.info('Tokenize batch: {}'.format(i))
            batch_df = self.load_clean()
            original_ids = batch_df[ID_FIELD_DF].tolist()
            if len(batch_df) < 1:
                break
            batch_df['tokens'] = batch_df['body'].apply(self.tokenize_text)
            del(batch_df['body'])
            sentiments_df = self.count_pos_neg(batch_df)
            self.write_sentiments(sentiments_df)
            self.delete_from_staging(original_ids)
            i += 1

    def count_pos_neg(self, df):
        positive = self.word_list[self.word_list['sentiment'] == 1]
        negative = self.word_list[self.word_list['sentiment'] == -1]
        undetermined = self.word_list[self.word_list['sentiment'] == 0]

        df['bullish'] = df['tokens'].apply(lambda x: len(set(x).intersection(set(positive['Word']))))
        df['bearish'] = df['tokens'].apply(lambda x: len(set(x).intersection(set(negative['Word']))))
        df['undetermined'] = df['tokens'].apply(lambda x: len(set(x).intersection(set(undetermined['Word']))))
        df = df[(df['bullish'] != 0) | (df['bearish'] != 0) | (df['undetermined'] != 0)]
        df.reset_index(drop=True)
        df['sentiment'] = df.apply(lambda x: sentiment(x['bullish'], x['bearish']), axis=1)
        return df

    def write_sentiments(self, df):
        logging.info('Writing to {}: {}'.format(self.sentiments_con.collection_name, str(len(df))))
        self.sentiments_con.insert_df(df)

    def delete_from_staging(self, df_ids):
        removed_result = self.clean_con.delete_by_id_in(df_ids)
        if hasattr(removed_result, 'deleted_count'):
            logging.info('Deleted from STAGING: {}'.format(removed_result.deleted_count))
        else:
            logging.info('Deleted from STAGING: count not found')

    def load_clean(self):
        df = self.clean_con.find(None, self.batch_limit)
        return df

    def load_words(self):
        return self.words_con.find(None)

    @staticmethod
    def tokenize_text(body):
        body = body.lower()
        body = BeautifulSoup(body, "html.parser").get_text()

        tokenizer = tokenize.RegexpTokenizer(r'\w+')
        body = tokenizer.tokenize(body)
        return [word for word in body if word not in corpus.stopwords.words('english')]


def sentiment(n, p):
    if n != 0:
        return -1
    elif p != 0:
        return 1
    else:
        return 0


def create_db_connectors(config):
    _uri = config[DB_TYPE][DB_URI]
    _db = config[DB_TYPE][DB_NAME]
    _clean = config[DB_TYPE][CLEAN_STAGING]
    _words = config[DB_TYPE][SENTIMENT_WORDS]
    _sentiments = config[DB_TYPE][SENTIMENTS]
    return {'words': DBConnector(DB_TYPE, _uri, _db, _words),
            'clean': DBConnector(DB_TYPE, _uri, _db, _clean),
            'sentiments': DBConnector(DB_TYPE, _uri, _db, _sentiments)}


def parse_args():
    arg_parser = ArgParser()
    arg_parser.add_config_path(True)
    arg_parser.add_path(True)
    return arg_parser.parse()


def get_config_values(config_path):
    return StarConfig(config_path, 'yaml').parse()


def main():
    try:
        log.setup_logger()
        logging.info('Sentiment analysis')
        if __debug__:
            pd.set_option('compute.use_bottleneck', True)
            pd.set_option('compute.use_numexpr', True)
            pd.set_option('display.width', 10000)
            pd.set_option('display.max_columns', 50)
            pd.set_option('display.expand_frame_repr', True)
            pd.set_option('display.max_colwidth', 1500)

        args = parse_args()
        config = get_config_values(args.config)

        db_connector_dict = create_db_connectors(config)
        wl = WordList(db_connector_dict)
        # wl.file_to_db(args.path)
        wl.determine_sentiments(config[DB_TYPE][BATCH_SIZE])

    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
