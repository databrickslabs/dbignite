from pyspark.sql import SparkSession

import os
import re

from dbignite.data_model import Transformer, PERSON_TABLE,CONDITION_TABLE, PROCEDURE_OCCURRENCE_TABLE, ENCOUNTER_TABLE
from dbignite.utils import *
from dbignite.schemas import *

REPO = os.environ.get('REPO', 'dbignite')
BRANCH = re.sub(r'\W+', '', os.environ['BRANCH'])
TEST_BUNDLE_PATH = './sampledata/'
TEST_DATABASE = f'test_{REPO}_{BRANCH}'


class SparkTest():
    ##
    ## Fixtures
    ##
    def setup_class(self):
        self.spark = (SparkSession.builder.appName("myapp") \
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                      .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                      .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                      .master("local") \
                      .getOrCreate())
        self.spark.conf.set("spark.sql.shuffle.partitions", 1)

    def teardown_class(self) -> None:
        self.spark.sql(f'DROP DATABASE IF EXISTS {TEST_DATABASE} CASCADE')

    def assertFieldsEqual(self, fieldA, fieldB):
        """
        Test that two fields are equivalent
        """
        assert fieldA.name.lower() == fieldB.name.lower()
        assert fieldA.dataType.simpleString() == fieldB.dataType.simpleString()

    def assertSchemaContainsField(self, schema, field):
        """
        Test that the given schema contains the given field
        """
        # the schema must contain a field with the right name
        lc_fieldNames = [fc.lower() for fc in schema.fieldNames()]
        assert field.name.lower() in lc_fieldNames
        # the attributes of the fields must be equal
        assert field==schema[field.name]

    def assertSchemasEqual(self, schemaA, schemaB):
        """
        Test that the two given schemas are equivalent (column ordering ignored)
        """
        # both schemas must have the same length
        assert len(schemaA.fields) == len(schemaB.fields)
        # schemaA must equal schemaB
        assert schemaA.simpleString() == schemaB.simpleString()

    def assertHasSchema(self, df, expectedSchema):
        """
        Test that the given Dataframe conforms to the expected schema
        """
        assert df.schema == expectedSchema

    def assertDataFramesEqual(self, dfA, dfB):
        """
        Test that the two given Dataframes are equivalent.
        That is, they have equivalent schemas, and both contain the same values
        """
        # must have the same schemas
        assert dfA.schema == dfB.schema
        # enforce a common column ordering
        colOrder = sorted(dfA.columns)
        sortedA = dfA.select(colOrder)
        sortedB = dfB.select(colOrder)
        # must have identical data
        # that is all rows in A must be in B, and vice-versa

        assert sortedA.subtract(sortedB).count()==0
        assert sortedB.subtract(sortedA).count() == 0

class TestUtils(SparkTest):

    def test_entries_to_person(self):
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        person_df = entries_to_person(entries_df)
        assert person_df.count() == 3
        self.assertSchemasEqual(person_df.schema,PERSON_SCHEMA)

    def test_entries_to_condition(self):
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        condition_df = entries_to_condition(entries_df)
        assert condition_df.count() == 103
        self.assertSchemasEqual(condition_df.schema,CONDITION_SCHEMA)

    def test_entries_to_procedure_occurrence(self):
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        procedure_occurrence_df = entries_to_procedure_occurrence(entries_df)
        assert procedure_occurrence_df.count()==119
        self.assertSchemasEqual(procedure_occurrence_df.schema,PROCEDURE_OCCURRENCE_SCHEMA)

    def test_entries_to_encounter(self):
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        encounter_df = entries_to_encounter(entries_df)
        assert encounter_df.count()==128
        self.assertSchemasEqual(encounter_df.schema,ENCOUNTER_SCHEMA)

class TestTransformers(SparkTest):

    def test_load_entries_df(self):
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        assert entries_df.count()==1872
        self.assertSchemasEqual(entries_df.schema,JSON_ENTRY_SCHEMA)

    def test_fhir_bundles_to_omop_cdm(self):
        omop_cdm = Transformer(self.spark).fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, True)
        tables = [t.tableName for t in self.spark.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()]

        assert TEST_DATABASE in omop_cdm.listDatabases()
        assert PERSON_TABLE in tables
        assert CONDITION_TABLE in tables
        assert PROCEDURE_OCCURRENCE_TABLE in tables
        assert ENCOUNTER_TABLE in tables

        assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

    # @unittest.skip("Not yet running as github action")
    def test_omop_cdm_to_person_dashboard(self):
        transformer=Transformer(self.spark)
        omop_cdm = transformer.fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH,TEST_DATABASE,None, True)
        person_dashboard = transformer.omop_cdm_to_person_dashboard(omop_cdm).summary()
        self.assertSchemasEqual(CONDITION_SUMMARY_SCHEMA,person_dashboard.select('conditions').schema)

## MAIN
if __name__ == '__main__':
    unittest.main()
#%%
