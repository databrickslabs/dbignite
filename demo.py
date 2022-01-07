# Databricks notebook source
# MAGIC %md 
# MAGIC The dbinterop package (temporarily stubbed out).
# MAGIC 
# MAGIC Going forward, this will be built in to a wheel file.

# COMMAND ----------

_cached_json = None # for demo purposes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from collections import namedtuple

# "import" dbinterop, this will become a "singleton" module
class DbInterop:
  
  fhir_df = None
  
  @staticmethod
  def parse_fhir_bundles(path: str, _cached_json=_cached_json) -> DataFrame:
    
    # Read and cache the raw json bundles
    if _cached_json is None:  
      df = spark.read.json(
        path, 
        multiLine=True,
      )
      DbInterop.fhir_df = df
      _cached_json = df
    else:
      df = _cached_json
      
    person_df = (
      DbInterop._parse_resources(df)
        .filter(col('entry.request.url') == 'Patient')
        .select(col("id"),
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
        .groupBy('id', 'gender_source_value', 'year_of_birth', 'month_of_birth', 'day_of_birth')
        .pivot('ext_field').agg(first("display"))
        .withColumnRenamed('us-core-ethnicity', 'ethnicity_source_value')
        .withColumnRenamed('us-core-race', 'race_source_value')
    )
    
    return person_df
  
  
  @staticmethod
  def _parse_resources(fhir_df):
    return (
      fhir_df
        .selectExpr('uuid() as id', 'entry')
        .selectExpr('id', 'explode(entry) as entry')
    )

dbinterop = DbInterop

# COMMAND ----------

# MAGIC %md ## Core Use Case: Quick Exploratory Analysis of a FHIR Bundle

# COMMAND ----------

path_to_my_fhir_bundles = 'dbfs:/FileStore/tables/fhirbundles/Synthea FHIR/'
display(dbutils.fs.ls(path_to_my_fhir_bundles))

# COMMAND ----------

# TODO: import dbinterop
exploratory_df = dbinterop.parse_fhir_bundles(path_to_my_fhir_bundles)

# COMMAND ----------

# Not part of the official spec.
# We have to find a place to keep the original df for comparison.
dbinterop.fhir_df.printSchema()

# COMMAND ----------

exploratory_df.printSchema()

# COMMAND ----------

# MAGIC %md > Note: Original FHIR DF fails to `display`

# COMMAND ----------

display(exploratory_df)

# COMMAND ----------


