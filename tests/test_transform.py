import unittest
from unittest import TestCase

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
  
import os

REPO = os.environ.get('REPO', 'dbignite')
BRANCH = os.environ['BRANCH']
TEST_BUNDLE_PATH = None

TEST_DATABASE = f'test_{REPO}_{BRANCH}'
BUNDLES_TABLE = 'bundles'

from pyspark.sql import SparkSession

spark = SparkSession \
 .builder \
 .appName("PyTest") \
 .getOrCreate()



@unittest.skip("Not yet running as github action")
class TestTransformers(TestCase):
  
  @classmethod
  def setUpClass(cls):
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {TEST_DATABASE}')
    spark.catalog.setCurrentDatabase(TEST_DATABASE)
    if BUNDLES_TABLE in (t.name for t in spark.catalog.listTables()):
      cls.bundles_df = spark.read.table(BUNDLES_TABLE)
    else:
      cls.bundles_df = read_fhir_bundles(TEST_BUNDLE_PATH)
      cls.bundles_df.writeTo(BUNDLES_TABLE).create()
      
  @classmethod
  def tearDownClass(cls):
    spark.catalog.setCurrentDatabase(TEST_DATABASE)
    spark.sql(f'DROP TABLE IF EXISTS {BUNDLES_TABLE}')
  
  def test_fhir_bundles_to_omop_cdm(self):
    omop_cdm = fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH)
    spark.catalog.setCurrentDatabase(omop_cdm.listDatabases()[0])
    spark.read.table(PERSON_TABLE).display()
    spark.read.table(CONDITION_TABLE).display()
    
  def test_fhir_bundles_to_person_dashbaord(self):
    person_dashboard = fhir_bundles_to_person_dashboard(TEST_BUNDLE_PATH)
    person_dashboard.summary().display()
