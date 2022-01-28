# Databricks notebook source
try:
  dbutils.widgets.text('repo', 'dbinterop')
  dbutils.widgets.text('branch', '')

  assert dbutils.widgets.get("branch") != ''
  
  REPO = dbutils.widgets.get('repo')
  BRANCH = dbutils.widgets.get('branch')
except NameError: # NameError: name 'dbutils' is not defined
  import os
  REPO = os.environ.get('REPO', 'dbinterop')
  BRANCH = os.environ['BRANCH']

# COMMAND ----------

# data_models/data_model.py
from abc import ABC, abstractmethod
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.catalog import Database

class DataModel(ABC):
  
  @abstractmethod
  def summary(self) -> DataFrame:
    ...
    
  @abstractmethod
  def listDatabases(self) -> Iterable[Database]:
    ...

# COMMAND ----------

# data_models/person_dashboard.py
from pyspark.sql import DataFrame

class PersonDashboard(DataModel):
  
  def __init__(self, df: DataFrame):
    self.df = df
    
  def summary(self):
    return self.df
  
  def listDatabases(self):
    raise NotImplementedError('TODO: persist the person dashboard')

# COMMAND ----------

# data_models/omop_cdm.py
from pyspark.sql import DataFrame

class OmopCdm(DataModel):
  
  def __init__(self, cdm_database: str, mapping_database: str):
    self.cdm_database = cdm_database
    self.mapping_database = mapping_database
    
  def summary(self) -> DataFrame:
    raise NotImplementedError('TODO: summarize OMOP CDM in a DataFrame')
  
  def listDatabases(self):
    return (self.cdm_database, self.mapping_database)

# COMMAND ----------

# transformers/fhir_bundles_to_person_dashboard.py
def fhir_bundles_to_person_dashboard(path: str) -> PersonDashboard:
  omop_cdm = fhir_bundles_to_omop_cdm(path)
  person_dashboard = omop_cdm_to_person_dashboard(*omop_cdm.listDatabases())
  return person_dashboard

# COMMAND ----------

# transformers/fhir_bundles_to_omop_cdm.py
import json
from copy import deepcopy

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import udf


PERSON_TABLE = 'person'
CONDITION_TABLE = 'condition'


ENTRY_SCHEMA = StructType([
  StructField('resource', StructType([
    StructField('id', StringType()),
    # Not named in the spec.
    StructField('resourceType', StringType()) 
  ])),
  StructField('request', StructType([
    # Not technically required, but named in spec.
    StructField('url',  StringType())
  ]))
])


def fhir_bundles_to_omop_cdm(path: str) -> OmopCdm:
  entries_df = (
    spark.read.text(path, wholetext=True)
    .select(explode(_entry_json_strings('value')).alias('entry_json'))
    .withColumn('entry', from_json('entry_json', schema=ENTRY_SCHEMA))
  ).cache()
  person_df = _entries_to_person(entries_df)
  condition_df = _entries_to_condition(entries_df)
  
  cdm_database = f'cdm_{REPO}_{BRANCH}'
  mapping_database = f'cdm_mapping_{REPO}_{BRANCH}'
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {cdm_database}')
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {mapping_database}')
  spark.catalog.setCurrentDatabase(cdm_database)
  person_df.writeTo(PERSON_TABLE).createOrReplace()
  condition_df.writeTo(CONDITION_TABLE).createOrReplace()
  return OmopCdm(cdm_database, mapping_database)


@udf(ArrayType(StringType()))
def _entry_json_strings(value):
  '''
  UDF takes raw text, returns the
  parsed struct and raw JSON.
  '''
  bundle_json = json.loads(value)
  return [json.dumps(e) for e in bundle_json['entry']]


def _entries_to_person(entries_df):
  entry_schema = deepcopy(ENTRY_SCHEMA)
  patient_schema = next(f.dataType for f in entry_schema.fields if f.name == 'resource')
  patient_schema.fields.extend([
    StructField('name', StringType()),
    StructField('gender', StringType()),
    StructField('birthDate', DateType()),
    StructField('address', StringType()),
    StructField('extension', ArrayType(StructType([
      StructField('url', StringType())
    ])))
  ])
  return (
    entries_df
    .where(col('entry.request.url') == 'Patient')
    .withColumn('patient', from_json('entry_json', schema=entry_schema)['resource'])
    .select(
      col('patient.id').alias('person_id'),
      col('patient.name').alias('name'),
      col('patient.gender').alias('gender_source_value'),
      year(col('patient.birthDate')).alias('year_of_birth'),
      month(col('patient.birthDate')).alias('month_of_birth'),
      dayofmonth(col('patient.birthDate')).alias('day_of_birth'),
      col('patient.address').alias('address'),
    )
  )


def _entries_to_condition(entries_df):
  entry_schema = deepcopy(ENTRY_SCHEMA)
  condition_schema = next(f.dataType for f in entry_schema.fields if f.name == 'resource')
  condition_schema.fields.extend([
    StructField('subject', StructType([
      StructField('reference', StringType())
    ])),
    StructField('encounter', StructType([
      StructField('reference', StringType())
    ])),
    StructField('code', StructType([
      StructField('coding', ArrayType(StructType([
        StructField('code', StringType()),
        StructField('display', StringType()),
      ]))),
      StructField('text', StringType())
    ])),
    StructField('onsetDateTime', TimestampType()),
    StructField('abatementDateTime', TimestampType()),
  ])
  return (
    entries_df
    .where(col('entry.request.url') == 'Condition')
    .withColumn('condition', from_json('entry_json', schema=entry_schema)['resource'])
    .select(
      col('condition.id').alias('condition_occurrence_id'),
      regexp_replace(col('condition.subject.reference'),'urn:uuid:','').alias('person_id'),
      regexp_replace(col('condition.encounter.reference'),'urn:uuid:','').alias('visit_occurrence_id'),
      col('condition.onsetDateTime').alias('condition_start_datetime'),
      col('condition.abatementDateTime').alias('condition_end_datetime'),
      col('condition.code.coding')[0]['display'].alias('condition_status'),
      col('condition.code.coding')[0]['code'].alias('condition_code'),
    )
  )

# COMMAND ----------

# transformers/omop_cdm_to_person_dashboard.py
def omop_cdm_to_person_dashboard(cdm_database: str, mapping_database: str) -> PersonDashboard:
  spark.catalog.setCurrentDatabase(cdm_database)
  person_df = spark.read.table(PERSON_TABLE)
  condition_df = spark.read.table(CONDITION_TABLE)
  condition_summary_df = _summarize_condition(condition_df)
  return PersonDashboard(
    person_df
    .join(condition_summary_df, 'person_id', 'left')
  )
  
def _summarize_condition(condition_df):
  return (
    condition_df
    .orderBy('condition_start_datetime')
    .select(col('person_id'), struct('*').alias('condition'))
    .groupBy('person_id')
    .agg(
      collect_list('condition').alias('conditions'),
    )
  )

# COMMAND ----------

# transformers/test.py
from unittest import TestCase

from pyspark.sql import DataFrame
from pyspark.sql.functions import *

TEST_BUNDLE_PATH = 'dbfs:/FileStore/tables/fhirbundles/Synthea FHIR/'
TEST_DATABASE = f'test_{REPO}_{BRANCH}'
BUNDLES_TABLE = 'bundles'
    
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

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Core Use Case: Quick Exploratory Analysis of a FHIR Bundle
# MAGIC 
# MAGIC The _fhir_bundles_to_person_dahsboard Transformer_ mimizes friction
# MAGIC when exploring FHIR bundles on Databricks. The _PersonDashboard DataModel_
# MAGIC simplifies and optimizes visualization of the persons in the data.

# COMMAND ----------

try:
  display(dbutils.fs.ls(TEST_BUNDLE_PATH))
except NameError: # NameError: name 'dbutils' is not defined
  pass

# COMMAND ----------

# MAGIC %sh head -n 200 "/dbfs/FileStore/tables/fhirbundles/Synthea FHIR/Abby_Orn_58becc37_14a9_0e14_5f98_2d1e4355ccaf.json"

# COMMAND ----------

# MAGIC %md 
# MAGIC > 
# MAGIC > Note: The original FHIR bundle DF fails has a schema that is not ideal for analytic consumption. 
# MAGIC >
# MAGIC > We are not even able to `display`.
# MAGIC > 

# COMMAND ----------

bundles_df = spark.read.json(path, multiLine=True)
bundles_df.printSchema()
bundles_df.display()

# COMMAND ----------

from pyspark.sql.functions import *

#import dbinterop
class dbinterop:
  DataModel = DataModel
  class transformers:
    fhir_bundles_to_person_dashboard = fhir_bundles_to_person_dashboard

# COMMAND ----------

person_dashboard = \
  dbinterop.transformers.fhir_bundles_to_person_dashboard(TEST_BUNDLE_PATH)
person_dashboard.summary().display()

# COMMAND ----------

person_dashboard: dbinterop.DataModel = \
  dbinterop.transformers.fhir_bundles_to_person_dashboard(TEST_BUNDLE_PATH)
  
(
  person_dashboard.summary()
  .withColumn(
    'history_of_diabetes', 
    when(
      exists('conditions', lambda c: lower(c['condition_status']).contains('diabetes')),
      1).otherwise(0))
  .withColumn(
    'history_of_stroke', 
    when(
      exists('conditions', lambda c: lower(c['condition_status']).contains('stroke')),
      1).otherwise(0))
  .withColumn(
    'history_of_bronchitis', 
    when(
      exists('conditions', lambda c: lower(c['condition_status']).contains('bronchitis')),
      1).otherwise(0))
).display()

# COMMAND ----------


