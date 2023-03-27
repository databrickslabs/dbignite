from pyspark.sql import SparkSession
import os, re, unittest

from chispa import *
from chispa.schema_comparer import *

from dbignite.data_model import *
from dbignite.utils import *
from dbignite.schemas import *

REPO = os.environ.get("REPO", "dbignite")
TEST_BUNDLE_PATH = "./sampledata/"
TEST_DATABASE = f"test_{REPO}"


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.spark = (
            SparkSession.builder
            .appName("myapp")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        self.fhirBundles = FhirBundles(path=TEST_BUNDLE_PATH)
        self.cdmModel = cdm_model=OmopCdm(TEST_DATABASE)
        self.entriesDF = self.fhirBundles.loadEntries()

    def tearDown(self):
        self.spark.stop()

    def test_entries_to_person(self):
        person_df = entries_to_person(self.entriesDF)
        assert person_df.count() == 3
        assert_schema_equality(person_df.schema, PERSON_SCHEMA, ignore_nullable=True)


    def test_entries_to_condition(self):
        condition_df = entries_to_condition(self.entriesDF)
        assert condition_df.count() == 103
        assert_schema_equality(condition_df.schema, CONDITION_SCHEMA,ignore_nullable=True)

    def test_entries_to_procedure_occurrence(self):
        procedure_occurrence_df = entries_to_procedure_occurrence(self.entriesDF)
        assert procedure_occurrence_df.count() == 119
        assert_schema_equality(procedure_occurrence_df.schema, PROCEDURE_OCCURRENCE_SCHEMA, ignore_nullable=True)

    def test_entries_to_encounter(self):
        encounter_df = entries_to_encounter(self.entriesDF)
        assert encounter_df.count() == 128
        assert_schema_equality(encounter_df.schema, ENCOUNTER_SCHEMA,ignore_nullable=True)


class TestTransformers(unittest.TestCase):

    def setUp(self):
        self.spark = (
            SparkSession.builder
            .appName("myapp")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        self.fhirModel = FhirBundles(TEST_BUNDLE_PATH)
        self.cdmModel = cdm_model=OmopCdm(TEST_DATABASE)
        self.entriesDF = self.fhirModel.loadEntries()

    def tearDown(self):
        self.spark.stop()


    def test_loadEntries(self):
        assert self.entriesDF.count() == 1872
        assert_schema_equality(self.entriesDF.schema, JSON_ENTRY_SCHEMA,ignore_nullable=True)

    def test_fhir_bundles_to_omop_cdm(self):
        FhirBundlesToCdm(self.spark).transform(fhirModel, cdmModel, True)
        tables = [t.tableName for t in self.spark.sql(f"SHOW TABLES FROM {TEST_DATABASE}").collect()]

        assert TEST_DATABASE in cdm_model.listDatabases()
        assert PERSON_TABLE in tables
        assert CONDITION_TABLE in tables
        assert PROCEDURE_OCCURRENCE_TABLE in tables
        assert ENCOUNTER_TABLE in tables

        assert self.spark.table(f"{TEST_DATABASE}.person").count() == 3

    def test_omop_cdm_to_person_dashboard(self):
        transformer = CdmToPersonDashboard(self.spark)
        person_dash_model=PersonDashboard()
        
        FhirBundlesToCdm(self.spark).transform(fhirModel, cdmModel, True)
        CdmToPersonDashboard(self.spark).transform(cdmModel,person_dash_model)
        person_dashboard_df=person_dash_model.summary()
        assert_schema_equality(CONDITION_SUMMARY_SCHEMA, person_dashboard_df.select('conditions').schema, ignore_nullable=True)
