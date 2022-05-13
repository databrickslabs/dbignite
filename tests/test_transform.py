import logging
import unittest

from unittest import TestCase

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *


import os

REPO = os.environ.get('REPO', 'dbignite')
BRANCH = os.environ['BRANCH']
TEST_BUNDLE_PATH = '/sampledata/'

TEST_DATABASE = f'test_{REPO}_{BRANCH}'
BUNDLES_TABLE = 'bundles'


# spark = SparkSession \
#  .builder \
#  .appName("PyTest") \
#  .getOrCreate()



import logging
import unittest

from unittest import TestCase

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os
import re

from dbignite.data_model import fhir_bundles_to_omop_cdm, OmopCdm
REPO = os.environ.get('REPO', 'dbignite')
BRANCH = re.sub(r'\W+', '', os.environ['BRANCH'])

TEST_DATABASE = f'test_{REPO}_{BRANCH}'
# BUNDLES_TABLE = 'bundles'


# @unittest.skip("Not yet running as github action")
class TestTransformers(TestCase):
  
  @classmethod
  def setUpClass(cls):
    cls.spark = SparkSession.builder.appName("PyTest").getOrCreate()
    cls.spark.sql(f'CREATE DATABASE IF NOT EXISTS {TEST_DATABASE}')
    cls.spark.catalog.setCurrentDatabase(TEST_DATABASE)
#     if BUNDLES_TABLE in (t.name for t in spark.catalog.listTables()):
#       cls.bundles_df = spark.read.table(BUNDLES_TABLE)
#     else:
#       cls.bundles_df = read_fhir_bundles(TEST_BUNDLE_PATH)
#       cls.bundles_df.writeTo(BUNDLES_TABLE).create()
      
  @classmethod
  def tearDownClass(cls):
    cls.spark.stop()
#   spark.catalog.setCurrentDatabase(TEST_DATABASE)
#     spark.sql(f'DROP TABLE IF EXISTS {BUNDLES_TABLE}')
  
#   def test_fhir_bundles_to_omop_cdm(self):
#     omop_cdm = fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None)
#     self.spark.catalog.setCurrentDatabase(omop_cdm.listDatabases()[0])
#     self.spark.read.table(PERSON_TABLE).display()
#     self.spark.read.table(CONDITION_TABLE).display()
    
  def test_fhir_bundles_to_omop_cdm(self):
    self.setUpClass()
    omop_cdm = fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, False)
    assert TEST_DATABASE in omop_cdm.listDatabases()
    logging.info('delta table count ' + str(self.spark.table("person").count()))
    assert self.spark.table(f"{TEST_DATABASE}.person").count() == 1156

## MAIN
if __name__ == '__main__':
    unittest.main()