# Databricks notebook source
# MAGIC %fs mv /FileStore/tables/dbinterop-1.0-py2.py3-none-any.whl /FileStore/jars/1a27c6ae_75dd_45fa_9259_add4d51a045c/dbinterop-1.0-py2.py3-none-any.whl

# COMMAND ----------

# MAGIC %pip install --force-reinstall /dbfs/FileStore/jars/1a27c6ae_75dd_45fa_9259_add4d51a045c/dbinterop-1.0-py2.py3-none-any.whl

# COMMAND ----------

import os

try:
  dbutils.widgets.text('repo', 'dbinterop')
  dbutils.widgets.text('branch', '')

  assert dbutils.widgets.get("branch") != ''
  
  os.environ['REPO'] = dbutils.widgets.get('repo')
  os.environ['BRANCH'] = dbutils.widgets.get('branch')
  TEST_BUNDLE_PATH = 'dbfs:/FileStore/tables/fhirbundles/Synthea FHIR/'
  
except NameError: # NameError: name 'dbutils' is not defined
  TEST_BUNDLE_PATH = None
  
  from pyspark.sql import SparkSession

  spark = SparkSession \
    .builder \
    .appName("PyTest") \
    .getOrCreate()
  
REPO = os.environ.get('REPO', 'dbinterop')
BRANCH = os.environ['BRANCH']

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

bundles_df = spark.read.json(TEST_BUNDLE_PATH, multiLine=True)
bundles_df.printSchema()
bundles_df.display()

# COMMAND ----------

from dbinterop.data_model import FhirBundles, PersonDashboard

person_dashboard = PersonDashboard.builder(from_=FhirBundles(TEST_BUNDLE_PATH))
person_dashboard.summary().display()

# COMMAND ----------

from pyspark.sql.functions import *

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
