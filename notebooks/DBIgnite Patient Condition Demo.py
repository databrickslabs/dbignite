# Databricks notebook source
# MAGIC %md # Install DBIgnite for FHIR 

# COMMAND ----------

# DBTITLE 1,Installing DBIgnite
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/dbignite-forked.git

# COMMAND ----------

# MAGIC %md # Read in FHIR Data (C-CDA Messages)

# COMMAND ----------

# DBTITLE 1,Read in Sample Data
from  dbignite.fhir_mapping_model import FhirSchemaModel
fhir_schema = FhirSchemaModel()
from pyspark.sql.functions import size,col, sum, when, explode, udf, flatten
import uuid
from dbignite.readers import read_from_directory
sample_data = "s3://hls-eng-data-public/data/synthea/fhir/fhir/*json"

bundle = read_from_directory(sample_data)
df = bundle.read_data()

# COMMAND ----------

# DBTITLE 1,Print Patient Schema
df.select(col("Patient")).printSchema()

# COMMAND ----------

# MAGIC %md # ETL Using Dataframe API
# MAGIC Working with Patient Data and Write Results to Tables
# MAGIC
# MAGIC Note: Synthetic data uses SNOWMED coding system. In Healthcare, ICD10 PCS, ICD CM, CPT4, HCPCS are the accepted codes

# COMMAND ----------

# DBTITLE 1,Patient Conditions Sample Data
from pyspark.sql.types import StringType 
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
from pyspark.sql.functions import size,col, sum, when, explode, expr, flatten

#assign a unique id to each FHIR bundle 
df = df.withColumn("bundleUUID", expr("uuid()"))

df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Condition")).select(col("Patient"), col("bundleUUID"), explode("Condition").alias("Condition")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("patient.birthDate").alias("Birth_date"),
  col("Condition.clinicalStatus.coding.code")[0].alias("clinical_status"),
  col("Condition.code.coding.code")[0].alias("condition_code"), #can use the explode() function to pivot a column into a row. i.e. one row per patient per condition
  col("Condition.code.coding.system")[0].alias("condition_type_code"), 
  col("Condition.code.text").alias("condition_description"),
  col("Condition.recordedDate").alias("condition_date") 
).filter(col("Patient").like("efee780e%") |  col("Patient").like("1a5e6090%")).show()
#Selecting 2 patients here. However, if this was the same patient in separate fhir bundles, you would be working with one row per FHIR bundle. So 2 patients in 2 FHIR bundles = 2 rows

# COMMAND ----------

# DBTITLE 1,Save Conditions as a Table

df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Condition")).select(col("Patient"), col("bundleUUID"), explode("Condition").alias("Condition")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("patient.birthDate").alias("Birth_date"),
  col("Condition.clinicalStatus.coding.code")[0].alias("clinical_status"),
  col("Condition.code.coding.code")[0].alias("condition_code"), #can use the explode() function to pivot a column into a row. i.e. one row per patient per condition
  col("Condition.code.coding.system")[0].alias("condition_type_code"), 
  col("Condition.code.text").alias("condition_description"),
  col("Condition.recordedDate").alias("condition_date") 
).write.mode("overwrite").saveAsTable("hls_dev.default.patient_conditions")

# COMMAND ----------

# DBTITLE 1,Claim Detail Sample Data

df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Claim")).select(col("Patient"), col("bundleUUID"), explode("Claim").alias("Claim")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("claim.patient").alias("claim_patient_id"),
  col("claim.id").alias("claim_id"),
  col("patient.birthDate").alias("Birth_date"),
  col("claim.type.coding.code")[0].alias("claim_type_cd"),
  col("claim.insurance.coverage")[0].alias("insurer"),
  col("claim.total.value").alias("claim_billed_amount"),
  col("claim.item.productOrService.coding.display").alias("prcdr_description"),
  col("claim.item.productOrService.coding.code").alias("prcdr_cd"),
  col("claim.item.productOrService.coding.system").alias("prcdr_coding_system")
).filter(col("Patient").like("efee780e%") |  col("Patient").like("1a5e6090%")).show()

# COMMAND ----------

# DBTITLE 1,Save Claims as a Table

df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("Claim")).select(col("Patient"), col("bundleUUID"), explode("Claim").alias("Claim")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("claim.patient").alias("claim_patient_id"),
  col("claim.id").alias("claim_id"),
  col("patient.birthDate").alias("Birth_date"),
  col("claim.type.coding.code")[0].alias("claim_type_cd"),
  col("claim.insurance.coverage")[0].alias("insurer"),
  col("claim.total.value").alias("claim_billed_amount"),
  col("claim.item.productOrService.coding.display").alias("prcdr_description"),
  col("claim.item.productOrService.coding.code").alias("prcdr_cd"),
  col("claim.item.productOrService.coding.system").alias("prcdr_coding_system")
).write.mode("overwrite").saveAsTable("hls_dev.default.patient_claims")

# COMMAND ----------

# MAGIC %md # ETL Using SQL 
# MAGIC Write FHIR as is to Table and Use SQL to manipulate

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS hls_dev.default.patient""")
spark.sql("""DROP TABLE IF EXISTS hls_dev.default.condition""")
spark.sql("""DROP TABLE IF EXISTS hls_dev.default.claim""")

# COMMAND ----------

df.select(col("bundleUUID"), col("Patient")).write.mode("overwrite").saveAsTable("hls_dev.default.patient")
df.select(col("bundleUUID"), col("Condition")).write.mode("overwrite").saveAsTable("hls_dev.default.condition")
df.select(col("bundleUUID"), col("Claim")).write.mode("overwrite").saveAsTable("hls_dev.default.claim")

# COMMAND ----------

# DBTITLE 1,Select Patient Condition Information
# MAGIC %sql
# MAGIC select p.bundleUUID as UNIQUE_FHIR_ID, 
# MAGIC   p.Patient.id,
# MAGIC   p.patient.birthDate,
# MAGIC   c.Condition.clinicalStatus.coding.code[0] as clinical_status,
# MAGIC   c.Condition.code.coding.code[0] as condition_code, 
# MAGIC   c.Condition.code.coding.system[0] as condition_type_code, 
# MAGIC   c.Condition.code.text as condition_description,
# MAGIC   c.Condition.recordedDate condition_date
# MAGIC from (select bundleUUID, explode(Patient) as patient from hls_dev.default.patient) p --all patient information
# MAGIC   inner join (select bundleUUID, explode(condition) as condition from hls_dev.default.condition) c --all conditions from that patient 
# MAGIC     on p.bundleUUID = c.bundleUUID --Only show records that were bundled together 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Select Claims Information
# MAGIC %sql
# MAGIC select p.bundleUUID as UNIQUE_FHIR_ID, 
# MAGIC   p.Patient.id as patient_id,
# MAGIC   p.patient.birthDate,
# MAGIC   c.claim.patient as claim_patient_id, --Note this column looks unstructed because it is an ambigious "reference" in the FHIR JSON schema. Can be customized  further as well 
# MAGIC   c.claim.id as claim_id,
# MAGIC   c.claim.type.coding.code[0] as claim_type_cd, --837I = Institutional, 837P = Professional
# MAGIC   c.claim.insurance.coverage[0],
# MAGIC   c.claim.total.value as claim_billed_amount,
# MAGIC   c.claim.item.productOrService.coding.display as procedure_description,
# MAGIC   c.claim.item.productOrService.coding.code as procedure_code,
# MAGIC   c.claim.item.productOrService.coding.system as procedure_coding_system
# MAGIC from (select bundleUUID, explode(Patient) as patient from hls_dev.default.patient) p --all patient information
# MAGIC   inner join (select bundleUUID, explode(claim) as claim from hls_dev.default.claim) c --all conditions from that patient 
# MAGIC     on p.bundleUUID = c.bundleUUID --Only show records that were bundled together 
