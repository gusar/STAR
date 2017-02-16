import pymongo as pm


class Mongo(object):

    def __init__(self, mongodb_uri=None, db_name=None, collection=None):
        self.client = pm.MongoClient(mongodb_uri, connect=True)

        self._db = None if db_name is None else self.set_db(db_name)
        self._collection = None if collection is None else self.set_collection(collection)

    def set_db(self, db_name):
        self._db = self.client[db_name]

    def set_collection(self, collection_name):
        if self._db is not None:
            self._collection = self._db[collection_name]
        else:
            raise MongoException("Database not defined")

    def insert(self, data):
        if self._collection:
            self._collection.insert(data)
        else:
            raise MongoNoneCollection

    def find(self):
        if self._collection:
            pass
        else:
            raise MongoNoneCollection

    @property
    def get_db(self):
        return self._db

    @property
    def get_collection(self):
        return self._collection


class MongoNoneCollection(Exception):
    def __init__(self):
        self.message = 'Collection not defined'
