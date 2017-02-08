import pymongo as pm


class Mongo:

    def __init__(self, mongodb_uri=None):
        self.client = pm.MongoClient(mongodb_uri, connect=True)
        self.db = None
        self.collection = None

    def set_db(self, db_name):
        self.db = self.client[db_name]

    def set_collection(self, collection_name):
        if self.db is not None:
            self.collection = self.db[collection_name]
        else:
            raise MongoException("Database not defined")

    def insert(self, data):
        if self.collection:
            self.collection.insert(data)
        else:
            raise MongoException("Collection not defined")


class MongoException(Exception):
    pass
