from dateutil.parser import parse as dp

from star.db.connector import DBConnector

class StockTwitsETL(object):
    def __init__(self):
        self.session = pyspark_session('star_raw.raw_staging', 'star_raw.raw_archive', name='StockTwits ETL')

    def string_to_date(self):
        df = self.session.read.format("com.mongodb.spark.sql").load()
        # df.registerTempTable("mycollection")
        # result_data = spark.sql("SELECT * from mycollection limit 10")

        func = udf(lambda x: dp(x), DateType())

        dfr = df.withColumn('postedT', func(col('object.postedTime')))

        dfr.registerTempTable("dfr")
        rf = self.session.sql("SELECT * from dfr limit 10")
        rf.show()

    def drop_columns(self):
        pass


@sparkenv
def main():
    stf = StockTwitsETL()


if __name__ == '__main__':
    main()
