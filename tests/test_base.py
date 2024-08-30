import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

class PysparkBaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("DBIgniteTests").config("spark.driver.memory", "8g").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        
