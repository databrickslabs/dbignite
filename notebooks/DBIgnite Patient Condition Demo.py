# Databricks notebook source
# DBTITLE 1,Installing DBIgnite
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/dbignite-forked.git

# COMMAND ----------

# DBTITLE 1,Read in Sample Data
from  dbignite.fhir_mapping_model import FhirSchemaModel
fhir_schema = FhirSchemaModel()
from pyspark.sql.functions import size,col, sum, when, explode, udf, flatten
import uuid
from dbignite.readers import read_from_directory
sample_data = "/home/amir.kermany@databricks.com/health-lakehouse/data/*json"

bundle = read_from_directory(sample_data)
df = bundle.read_data()

# COMMAND ----------

# DBTITLE 1,Print Patient Schema
# MAGIC %python
# MAGIC df.select(col("Patient")).printSchema()

# COMMAND ----------

df.select(col("Condition")).printSchema()

# COMMAND ----------

from pyspark.sql.types import StringType 
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
from pyspark.sql.functions import size,col, sum, when, explode, udf, flatten


df.withColumn("bundleUUID",uuidUdf()).select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Condition").alias("Condition")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("patient.birthDate").alias("Birth_date"),
  flatten(col("Condition.clinicalStatus.coding.code")).alias("clinical_status"),
  flatten(col("Condition.verificationStatus.coding.code")).alias("verified_status"),
  flatten(col("Condition.code.coding.code")).alias("condition_code"), #can use the explode() function to pivot a column into a row. i.e. one row per patient per condition
  flatten(col("Condition.code.coding.system")).alias("condition_type_code"), 
  col("Condition.code.text").alias("display"),
  col("Condition.recordedDate").alias("dx_date") 
).filter(col("Patient").like("efee780e%") |  col("Patient").like("1a5e6090%")).show()
#Selecting 2 patients here. However, if this was the same patient in separate fhir bundles, you would be working with one row per FHIR bundle. So 2 patients in 2 FHIR bundles = 2 rows

# COMMAND ----------

#Save this result to a table for downstream use...

df.withColumn("bundleUUID",uuidUdf()).select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Condition").alias("Condition")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("patient.birthDate").alias("Birth_date"),
  flatten(col("Condition.clinicalStatus.coding.code")).alias("clinical_status"),
  flatten(col("Condition.verificationStatus.coding.code")).alias("verified_status"),
  flatten(col("Condition.code.coding.code")).alias("condition_code"),
  flatten(col("Condition.code.coding.system")).alias("condition_type_code"),
  col("Condition.code.text").alias("display"),
  col("Condition.recordedDate").alias("dx_date")
).write.saveAsTable("hls_dev.default.person_condition_table")
