import unittest

try:
    from star.spark.spark import sparkenv
    from pyspark import SparkContext


    class MyTestCase(unittest.TestCase):

        @sparkenv
        def test_pyspark_parallel(self):
            sc = SparkContext('local')

            test_words = sc.parallelize(["scala", "java", "hadoop", "spark", "akka"])

            self.assertEqual(test_words.count(), 5)
except ImportError:
    pass


if __name__ == '__main__':
    unittest.main()
