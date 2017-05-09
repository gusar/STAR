from dateutil.parser import parse as dp

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType
from star.spark.spark import sparkenv


class MongoSpark(object):

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



@sparkenv
def main():
    spark = (SparkSession.builder
             .appName('etl')
             .master("spark://star:7077")
             .config("spark.mongodb.input.uri", "mongodb://localhost:27017/star_raw.raw_staging")
             .getOrCreate())
    # spark.conf.set("spark.mongodb.input.uri", "mongodb://localhost:27017/star_raw.raw_staging")

    df = spark.read.format("com.mongodb.spark.sql").load()
    # df.registerTempTable("mycollection")
    # result_data = spark.sql("SELECT * from mycollection limit 10")

    func = udf(lambda x: dp(x), DateType())

    dfr = df.withColumn('postedT', func(col('object.postedTime')))

    dfr.registerTempTable("dfr")
    rf = spark.sql("SELECT * from dfr limit 10")
    rf.show()

    print(1)


if __name__ == '__main__':
    main()

