import pymongo as pm

from star.db.mongo_query import MongoQueryBuilder
from star.utils import pandas_utils


class Mongo(object):

    def __init__(self, mongodb_uri, db_name, collection):
        self.client = pm.MongoClient(mongodb_uri, connect=True)

        self._db = None
        self._collection = None

        self.set_db(db_name)
        self.set_collection(collection)
        self.query_builder = MongoQueryBuilder(self)

    def set_db(self, db_name):
        """
        Set database for the MongoClient instance
        :param db_name: str
        """
        self._db = self.client[db_name]

    def set_collection(self, collection_name):
        """
        Set collection name for the MongoClient instance
        :param collection_name: str
        """
        if self._db is not None:
            self._collection = self._db[collection_name]
        else:
            raise MongoNoneCollection("Database not defined")

    def insert(self, data):
        """
        Insert documents into pre-defined collection
        :param data: dict or iterable
        """
        if self._collection:
            self._collection.insert(data)
        else:
            raise MongoNoneCollection

    def find(self, query={}, limit=None):
        """
        Perform find operation on pre-fedined collection
        :param query: str: mongodb query format, default: {}
        :param limit: int
        :return: DataFrame
        """
        if self._collection is None:
            raise MongoNoneCollection
        if limit is None:
            cursor = self._collection.find(query)
        else:
            cursor = self._collection.find(query).limit(limit)
        return pandas_utils.mongo_to_df(cursor)

    @property
    def get_db(self):
        return self._db

    @property
    def get_collection(self):
        return self._collection


class MongoNoneCollection(Exception):
    def __init__(self):
        self.message = 'Collection not defined'
