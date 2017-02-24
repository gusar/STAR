import os
import sys


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
        except ImportError as err:
            raise err('Spark wrapper could not import the required libraries')
            sys.exit(-1)

        return func(*args, **kwargs)
    return set_spark_env_and_call
