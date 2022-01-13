# Databricks notebook source
# MAGIC %md 
# MAGIC The dbinterop package (temporarily stubbed out).
# MAGIC 
# MAGIC Going forward, this will be built in to a wheel file.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import DataFrame, Window
from collections import namedtuple

# "import" dbinterop, this will become a "singleton" module
class DbInteropTestFixture:
  bundle_df = None
  fhir_resources_df = None
  
dbinterop = DbInteropTestFixture

# COMMAND ----------

def parse_fhir_bundles(path: str) -> DataFrame:
  bundle_df = spark.read.json(
    path, 
    multiLine=True,
  )
  DbInteropTestFixture.bundle_df = bundle_df

  fhir_resources_df = DbInteropTestFixture._parse_fhir_resources(bundle_df).cache()
  DbInteropTestFixture.fhir_resources_df = fhir_resources_df
  person_df = DbInteropTestFixture._parse_person(fhir_resources_df)
  procedure_df = DbInteropTestFixture._parse_procedure(fhir_resources_df)
  
  analytic_record = (
    person_df
    .join(procedure_df, 'person_id', how='left')
  )

  return analytic_record

DbInteropTestFixture.parse_fhir_bundles = parse_fhir_bundles

# COMMAND ----------

def _parse_fhir_resources(bundle_df) -> DataFrame:
  fhir_resource_df = (
    bundle_df
      .selectExpr('uuid() as id', 'entry')
      .selectExpr('id', 'explode(entry) as entry')
  )
  return fhir_resource_df

DbInteropTestFixture._parse_fhir_resources = _parse_fhir_resources

# COMMAND ----------

def _parse_person(fhir_resources_df):
  df = (
    fhir_resources_df
    .filter(col('entry.request.url') == 'Patient')
    .select(col('id').alias('person_id'),
      col('entry.resource.gender').alias('gender_source_value'),
      year(col('entry.resource.birthDate').cast('date')).alias('year_of_birth'),
      month(col('entry.resource.birthDate').cast('date')).alias('month_of_birth'),
      dayofmonth(col('entry.resource.birthDate').cast('date')).alias('day_of_birth'),
      explode(col('entry.resource.extension')).alias('ext'))
    .filter(col('ext.url').contains('us-core-race') | col('ext.url').contains('us-core-ethnicity'))
    .select(col('*'), 
      col('ext.url'), 
      explode(col('ext.extension')).alias('value'))
    .withColumn('ext_field', reverse(split(col('url').cast('string'), '/')).getItem(0))
    .select(col('*'), 
      col('value.valueCoding.code'), 
      col('value.valueCoding.display'))
    .filter((col('code') != 'null') & (col('display') != 'null'))
    .groupBy('person_id', 'gender_source_value', 'year_of_birth', 'month_of_birth', 'day_of_birth')
    .pivot('ext_field').agg(first("display"))
    .withColumnRenamed('us-core-ethnicity', 'ethnicity_source_value')
    .withColumnRenamed('us-core-race', 'race_source_value')
  )
  return df

DbInteropTestFixture._parse_person = _parse_person

# COMMAND ----------

def _summarize_procedure(fhir_resources_df):
  
  procedure_df = (
    fhir_resources_df
    .filter(col('entry.request.url') == 'Procedure')
    .select(
      col('id').alias('bundle_id'),
      col('entry.resource.id').alias('procedure_occurrence_id'),
      regexp_replace(col('entry.resource.subject.reference'), 'urn:uuid:', '').alias('person_id'),
      #(col('entry.resource.code.coding')[0].code).alias('procedure_concept_id'),
      #col('entry.resource.performedPeriod.start').cast('date').alias('procedure_start_date'), 
      col('entry.resource.performedPeriod.start').alias('procedure_start_datetime'), 
      #col('entry.resource.performedPeriod.end').cast('date').alias('procedure_end_date'), 
      col('entry.resource.performedPeriod.end').alias('procedure_end_datetime'), 
      #col('entry.resource.category').alias('procedure_type'),
      #col('entry.resource.performer').alias('provider_id'),
      regexp_replace(col('entry.resource.encounter.reference'), 'urn:uuid:', '').alias('visit_occurence_id'), 
      # from_json("entry.resource.location", location_schema).alias("location"),
     )
  )
  
  person_summary_df = (
    procedure_df
    .orderBy('procedure_start_datetime')
    .groupBy('person_id')
    .first('procedure_start_datetime').alias('first_procedure_start_datetime')
    .first('procedure_end_datetime').alias('first_procedure_end_datetime')
    .first('visit_occurence_id').alias('first_visit_occurence_id')
  )
  
  return person_summary_df

DbInteropTestFixture._parse_procedure = _parse_procedure

# COMMAND ----------

def test_summarize_procedure_displays():
  df = DbInteropTestFixture._summarize_procedure(DbInteropTestFixture.fhir_resources_df)
  display(df)
  
test_parse_procedure_displays()

# COMMAND ----------

# MAGIC %md ## Core Use Case: Quick Exploratory Analysis of a FHIR Bundle

# COMMAND ----------

path_to_my_fhir_bundles = 'dbfs:/FileStore/tables/fhirbundles/Synthea FHIR/'
display(dbutils.fs.ls(path_to_my_fhir_bundles))

# COMMAND ----------

# MAGIC %md 
# MAGIC > 
# MAGIC > Note: The original FHIR bundle DF fails has a schema that is not ideal for analytic consumption. 
# MAGIC >
# MAGIC > We are not even able to `display`.
# MAGIC > 

# COMMAND ----------

# Not part of the official spec.
# We have to find a place to keep the original df for comparison.
dbinterop.bundle_df.printSchema()

# COMMAND ----------

display(dbinterop.bundle_df)

# COMMAND ----------

# TODO: import dbinterop
exploratory_df = dbinterop.parse_fhir_bundles(path_to_my_fhir_bundles)

# COMMAND ----------

display(exploratory_df)
