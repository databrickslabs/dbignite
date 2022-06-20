from pyspark.sql import SparkSession
import os
import re

from dbignite.data_model import Transformer, PERSON_TABLE, CONDITION_TABLE, PROCEDURE_OCCURRENCE_TABLE, ENCOUNTER_TABLE
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
    def setup_class(self) -> None:
        self.spark = (SparkSession.builder.appName("myapp")
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0")
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                      .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
                      .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
                      .master("local")
                      .getOrCreate())
        self.spark.conf.set("spark.sql.shuffle.partitions", 1)

    def teardown_class(self) -> None:
        self.spark.sql(f'DROP DATABASE IF EXISTS {TEST_DATABASE} CASCADE')

    def assertSchemasEqual(self, schemaA: StructType, schemaB: StructType) -> None:
        """
        Test that the two given schemas are equivalent (column ordering ignored)
        """
        # both schemas must have the same length
        assert len(schemaA.fields) == len(schemaB.fields)
        # schemaA must equal schemaB
        assert schemaA.simpleString() == schemaB.simpleString()

    def assertHasSchema(self, df: DataFrame, expectedSchema: StructType) -> None:
        """
        Test that the given Dataframe conforms to the expected schema
        """
        assert df.schema == expectedSchema

    def assertDataFramesEqual(self, dfA: DataFrame, dfB: DataFrame) -> None:
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

        assert sortedA.subtract(sortedB).count() == 0
        assert sortedB.subtract(sortedA).count() == 0


class TestUtils(SparkTest):

    def test_entries_to_person(self) -> None:
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        person_df = entries_df.transform(entries_to_person)
        assert person_df.count() == 3
        self.assertSchemasEqual(person_df.schema, PERSON_SCHEMA)

    def test_entries_to_condition(self) -> None:
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        condition_df = entries_df.transform(entries_to_condition)
        assert condition_df.count() == 103
        self.assertSchemasEqual(condition_df.schema, CONDITION_SCHEMA)

    def test_entries_to_procedure_occurrence(self) -> None:
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        procedure_occurrence_df = entries_df.transform(entries_to_procedure_occurrence)
        assert procedure_occurrence_df.count() == 119
        self.assertSchemasEqual(procedure_occurrence_df.schema, PROCEDURE_OCCURRENCE_SCHEMA)

    def test_entries_to_encounter(self) -> None:
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        encounter_df = entries_df.transform(entries_to_encounter)
        assert encounter_df.count() == 128
        self.assertSchemasEqual(encounter_df.schema, ENCOUNTER_SCHEMA)

    def test_summarize_condition(self) -> None:
        entries_df = Transformer(spark).load_entries_df(TEST_BUNDLE_PATH)
        summarize_condition_df = entries_df.transform(entries_to_condition).transform(summarize_condition)
        summarize_condition_first = summarize_condition_df.first()
        assert summarize_condition_df.count() == 3
        assert summarize_condition_first.person_id == '4a0bf980-a2c9-36d6-da55-14d7aa5a85d9'
        assert summarize_condition_first.conditions[0].condition_occurrence_id == 'a6425765-fb3a-3b1a-161b-4b5ad0fe0495'
        assert summarize_condition_first.conditions[0].visit_occurrence_id == 'a6118cdf-24ea-94fe-6a6e-30e91c841e5a'
        assert summarize_condition_first.conditions[0].condition_status == 'Hypertension'

    def test_summarize_procedure_occurrence(self) -> None:
        entries_df = Transformer(spark).load_entries_df(TEST_BUNDLE_PATH)
        summarize_procedure_occurrence_df = entries_df.transform(entries_to_procedure_occurrence).transform(
            summarize_procedure_occurrence)
        summarize_procedure_occurrence_first = summarize_procedure_occurrence_df.first()
        assert summarize_procedure_occurrence_df.count() == 3
        assert summarize_procedure_occurrence_first.person_id == '4a0bf980-a2c9-36d6-da55-14d7aa5a85d9'
        assert summarize_procedure_occurrence_first.procedure_occurrences[
                   0].procedure_occurrence_id == '562ad62b-0671-c044-8dd1-4f60ad010bec'
        assert len(summarize_procedure_occurrence_first) == 2

    def test_summarize_encounter(self) -> None:
        entries_df = Transformer(spark).load_entries_df(TEST_BUNDLE_PATH)
        summarize_encounter_df = entries_df.transform(entries_to_encounter).transform(summarize_encounter)
        summarize_encounter_first = summarize_encounter_df.first()
        assert summarize_encounter_df.count() == 3
        assert len(summarize_encounter_first.encounters) == 76
        assert summarize_encounter_first.person_id == '4a0bf980-a2c9-36d6-da55-14d7aa5a85d9'
        assert summarize_encounter_first.encounters[1].encounter_id == '83075051-3b84-b92a-cefb-66497ee9d602'
        assert summarize_encounter_first.encounters[1].location[
                   0].location.display == 'BETH ISRAEL DEACONESS HOSPITAL-MILTON INC'


# summarize_encounter

class TestTransformers(SparkTest):

    def test_load_entries_df(self) -> None:
        entries_df = Transformer(self.spark).load_entries_df(TEST_BUNDLE_PATH)
        assert entries_df.count() == 1872
        self.assertSchemasEqual(entries_df.schema, JSON_ENTRY_SCHEMA)

    def test_fhir_bundles_to_omop_cdm(self) -> None:
        omop_cdm = Transformer(self.spark).fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH, TEST_DATABASE, None, True)
        tables = [t.tableName for t in self.spark.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()]

        assert TEST_DATABASE in omop_cdm.listDatabases()
        assert PERSON_TABLE in tables
        assert CONDITION_TABLE in tables
        assert PROCEDURE_OCCURRENCE_TABLE in tables
        assert ENCOUNTER_TABLE in tables

        assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

    # @unittest.skip("Not yet running as github action")
    def test_omop_cdm_to_person_dashboard(self) -> None:
        transformer = Transformer(self.spark)
        omop_cdm = transformer.fhir_bundles_to_omop_cdm(TEST_BUNDLE_PATH, TEST_DATABASE, None, True)
        person_dashboard = transformer.omop_cdm_to_person_dashboard(omop_cdm).summary()
        self.assertSchemasEqual(CONDITION_SUMMARY_SCHEMA, person_dashboard.select('conditions').schema)


## MAIN
if __name__ == '__main__':
    unittest.main()
# %%
