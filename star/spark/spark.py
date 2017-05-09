import os
import sys
from pyspark.sql import SparkSession


def sparkenv(func):
    """
    Wrapper for initialising correct environment variables in order to run Pyspark
    """

    def set_spark_env_and_call(*args, **kwargs):
        try:
            from pyspark import SparkConf

            os.environ['SPARK_HOME'] = "/home/andy/spark"
            os.environ['PYSPARK_PYTHON'] = "/home/andy/anaconda3/envs/star/bin/python3.5"
            os.environ['PYSPARK_DRIVER_PYTHON'] = "/home/andy/anaconda3/envs/star/bin/python3.5"
            # os.environ['PYSPARK_SUBMIT_ARGS'] = "--master star://master:8080"
        except ImportError as err:
            raise err('Spark wrapper could not import the required libraries')
            sys.exit(-1)

        return func(*args, **kwargs)

    return set_spark_env_and_call


def pyspark_session(input_collection, output_collection, master="spark://star:7077", name=None):
    """
    Create a Pyspark session
    :param input_collection: database_name.collection_name: str
    :param output_collection: database_name.collection_name: str
    :param master: master node url
    :param name: app name
    :return: pyspark context
    """
    return (SparkSession.builder
            .appName(name)
            .master(master)
            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/" + input_collection)
            .config("spark.mongodb.output.uri", "mongodb://localhost:27017/" + output_collection)
            .getOrCreate())
