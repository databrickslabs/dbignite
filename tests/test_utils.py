from pyspark.sql import SparkSession
import os
import re

from dbignite.data_model import *
from dbignite.utils import *
from dbignite.schemas import *

REPO = os.environ.get("REPO", "dbignite")
BRANCH = re.sub(r"\W+", "", os.environ["BRANCH"])
TEST_BUNDLE_PATH = "./sampledata/"
TEST_DATABASE = f"test_{REPO}_{BRANCH}"


##TODO Add content-based data tests

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
        entries_df = FhirBundlesToCdm(self.spark).loadEntries(TEST_BUNDLE_PATH)
        person_df = entries_to_person(entries_df)
        assert person_df.count() == 3
        self.assertSchemasEqual(person_df.schema, PERSON_SCHEMA)

    def test_entries_to_condition(self) -> None:
        entries_df = FhirBundlesToCdm(self.spark).loadEntries(TEST_BUNDLE_PATH)
        condition_df = entries_to_condition(entries_df)
        assert condition_df.count() == 103
        self.assertSchemasEqual(condition_df.schema, CONDITION_SCHEMA)

    def test_entries_to_procedure_occurrence(self) -> None:
        entries_df = FhirBundlesToCdm(self.spark).loadEntries(TEST_BUNDLE_PATH)
        procedure_occurrence_df = entries_to_procedure_occurrence(entries_df)
        assert procedure_occurrence_df.count() == 119
        self.assertSchemasEqual(procedure_occurrence_df.schema, PROCEDURE_OCCURRENCE_SCHEMA)

    def test_entries_to_encounter(self) -> None:
        entries_df = FhirBundlesToCdm(self.spark).loadEntries(TEST_BUNDLE_PATH)
        encounter_df = entries_to_encounter(entries_df)
        assert encounter_df.count() == 128
        self.assertSchemasEqual(encounter_df.schema, ENCOUNTER_SCHEMA)


class TestTransformers(SparkTest):

    def test_loadEntries(self) -> None:
        entries_df = FhirBundlesToCdm(self.spark).loadEntries(TEST_BUNDLE_PATH)
        assert entries_df.count() == 1872
        self.assertSchemasEqual(entries_df.schema, JSON_ENTRY_SCHEMA)

    def test_fhir_bundles_to_omop_cdm(self) -> None:
        fhir_model=FhirBundles(TEST_BUNDLE_PATH)
        cdm_model=OmopCdm(TEST_DATABASE)
        FhirBundlesToCdm(self.spark).transform(fhir_model, cdm_model, True)
        tables = [t.tableName for t in self.spark.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()]

        assert TEST_DATABASE in cdm_model.listDatabases()
        assert PERSON_TABLE in tables
        assert CONDITION_TABLE in tables
        assert PROCEDURE_OCCURRENCE_TABLE in tables
        assert ENCOUNTER_TABLE in tables

        assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

    def test_omop_cdm_to_person_dashboard(self) -> None:
        transformer = CdmToPersonDashboard(self.spark)
        fhir_model=FhirBundles(TEST_BUNDLE_PATH)
        cdm_model=OmopCdm(TEST_DATABASE)
        person_dash_model=PersonDashboard()
        
        FhirBundlesToCdm(self.spark).transform(fhir_model, cdm_model, True)
        CdmToPersonDashboard(self.spark).transform(cdm_model,person_dash_model)
        person_dashboard_df=person_dash_model.summary()
        self.assertSchemasEqual(CONDITION_SUMMARY_SCHEMA, person_dashboard_df.select('conditions').schema)
