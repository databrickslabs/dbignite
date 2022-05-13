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
TEST_BUNDLE_PATH = '../sampledata/'
TEST_DATABASE = f'test_{REPO}_{BRANCH}'




# @unittest.skip("Not yet running as github action")
class TestTransformers(TestCase):
  
  @classmethod
  def setUpClass(cls):
    cls.spark = SparkSession.builder.appName("PyTest").getOrCreate()
    cls.spark.sql(f'CREATE DATABASE IF NOT EXISTS {TEST_DATABASE}')
    cls.spark.catalog.setCurrentDatabase(TEST_DATABASE)

  @classmethod
  def tearDownClass(cls):
    cls.spark.stop()

  def test_fhir_bundles_to_omop_cdm(self):
    self.setUpClass()
    omop_cdm = fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, False)
    assert TEST_DATABASE in omop_cdm.listDatabases()
    logging.info('delta table count ' + str(self.spark.table("person").count()))
    assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

## MAIN
if __name__ == '__main__':
    unittest.main()