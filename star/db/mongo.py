import pymongo as pm

from star.utils import pandas_utils

from bson.objectid import ObjectId


class Mongo(object):

    def __init__(self, mongodb_uri, db_name, collection):
        self.client = pm.MongoClient(mongodb_uri, connect=True)
        self.collection_name = collection

        self._db = None
        self._collection = None

        self.set_db(db_name)
        self.set_collection(collection)

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
            raise MongoNoneCollection()

    def insert_df(self, df):
        """
        Insert documents into pre-defined collection
        :param df: dict or iterable
        """
        if '_id_$oid' in df.columns:
            del (df['_id_$oid'])
        if self._collection:
            self._collection.insert(df.to_dict('records'))
        else:
            raise MongoNoneCollection

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
        Perform find operation on pre-defined collection
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

    def find_between_dates(self, from_date, to_date, field):
        if self._collection is None:
            raise MongoNoneCollection
        cursor = self._collection.find({field: {"$gte": from_date, "$lt": to_date}})
        return pandas_utils.mongo_to_df(cursor)

    def find_distinct_list(self, id_list, id_field):
        """
        Match a list of field values against database.
        :param id_list: list of strings
        :param id_field: str
        :return: list of strings
        """
        return self._collection.find({id_field: {'$in': id_list}}).distinct(id_field)

    def delete_by_id_in(self, id_list):
        """
        Match a list of field values against database.
        :param id_list: list of strings
        :return: list of strings
        """
        id_list_bson = [ObjectId(x) for x in id_list]
        return self._collection.delete_many({'_id': {'$in': id_list_bson}})

    @property
    def get_db(self):
        return self._db

    @property
    def get_collection(self):
        return self._collection


class MongoNoneCollection(Exception):
    def __init__(self):
        self.message = 'Collection not defined'
