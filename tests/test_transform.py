import logging
import unittest

from unittest import TestCase

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *


import os
import re

from dbignite.data_model import Transformer, OmopCdm, PERSON_TABLE,CONDITION_TABLE, PROCEDURE_OCCURRENCE_TABLE, ENCOUNTER_TABLE

REPO = os.environ.get('REPO', 'dbignite')
BRANCH = re.sub(r'\W+', '', os.environ['BRANCH'])
TEST_BUNDLE_PATH = './sampledata/'
TEST_DATABASE = f'test_{REPO}_{BRANCH}'


CONDITION_SCMEA = StructType([StructField('conditions',ArrayType(StructType([
                    StructField('condition_occurrence_id',StringType(),'true'),
                    StructField('person_id',StringType(),'true'),
                    StructField('visit_occurrence_id',StringType(),'true'),
                    StructField('condition_start_datetime',TimestampType(),'true'),
                    StructField('condition_end_datetime',TimestampType(),'true'),
                    StructField('condition_status',StringType(),'true'),
                    StructField('condition_code',StringType(),'true')
                ]), 'false'),'true')])

class SparkTest(TestCase):
    ##
    ## Fixtures
    ##
    def setUp(self):
        self.spark = (SparkSession.builder.appName("myapp") \
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                      .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                      .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                      .master("local") \
                      .getOrCreate())
        self.spark.conf.set("spark.sql.shuffle.partitions", 1)

    def tearDown(self) -> None:
        self.spark.stop()

    
    def assertFieldsEqual(self, fieldA, fieldB):
        """
        Test that two fields are equivalent
        """
        self.assertEqual(fieldA.name.lower(), fieldB.name.lower())
        self.assertEqual(fieldA.dataType.simpleString(), fieldB.dataType.simpleString())

    def assertSchemaContainsField(self, schema, field):
        """
        Test that the given schema contains the given field
        """
        # the schema must contain a field with the right name
        lc_fieldNames = [fc.lower() for fc in schema.fieldNames()]
        self.assertTrue(field.name.lower() in lc_fieldNames)
        # the attributes of the fields must be equal
        self.assertFieldsEqual(field, schema[field.name])

    def assertSchemasEqual(self, schemaA, schemaB):
        """
        Test that the two given schemas are equivalent (column ordering ignored)
        """
        # both schemas must have the same length
        self.assertEqual(len(schemaA.fields), len(schemaB.fields))
        # schemaA must contain every field in schemaB
        for field in schemaB.fields:
            self.assertSchemaContainsField(schemaA, field)

    def assertHasSchema(self, df, expectedSchema):
        """
        Test that the given Dataframe conforms to the expected schema
        """
        self.assertSchemasEqual(df.schema, expectedSchema)

    def assertDataFramesEqual(self, dfA, dfB):
        """
        Test that the two given Dataframes are equivalent.
        That is, they have equivalent schemas, and both contain the same values
        """
        # must have the same schemas
        self.assertSchemasEqual(dfA.schema, dfB.schema)
        # enforce a common column ordering
        colOrder = sorted(dfA.columns)
        sortedA = dfA.select(colOrder)
        sortedB = dfB.select(colOrder)
        # must have identical data
        # that is all rows in A must be in B, and vice-versa

        self.assertEqual(sortedA.subtract(sortedB).count(), 0)
        self.assertEqual(sortedB.subtract(sortedA).count(), 0)

class TestTransformers(SparkTest):
  
  def test_fhir_bundles_to_omop_cdm(self):
    omop_cdm = Transformer(self.spark).fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, True)
    tables = [t.tableName for t in self.spark.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()]

    assert TEST_DATABASE in omop_cdm.listDatabases()
    assert PERSON_TABLE in tables
    assert CONDITION_TABLE in tables
    assert PROCEDURE_OCCURRENCE_TABLE in tables
    assert ENCOUNTER_TABLE in tables

    assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

  def test_debug_testing(self):
    omop_cdm = Transformer(self.spark).fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, True)
    tables = [t.tableName for t in self.spark.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()]

    assert TEST_DATABASE in omop_cdm.listDatabases()
    assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

  @unittest.skip("Not yet running as github action")
  def test_omop_cdm_to_person_dashboard(self):
    transformer=Transformer(self.spark)
    omop_cdm = transformer.fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, True)
    person_dashboard = transformer.omop_cdm_to_person_dashboard(*omop_cdm.listDatabases()).summary()
    self.assertSchemasEqual(CONDITION_SCMEA,person_dashboard.select('conditions').schema)

## MAIN
if __name__ == '__main__':
    unittest.main()