# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

pip install git+https://github.com/databricks-industry-solutions/dbignite.git@feature-FHIR-schema-dbignite-HLSSA-294

# COMMAND ----------

from dbignite.fhir_mapping_model import *

# COMMAND ----------

fhir_resource_map = FhirSchemaModel()

# COMMAND ----------

fhir_resource_map.schema("Account")

# COMMAND ----------

data = json.load(open("../sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json", "r"))
abe = data['entry'][0]['resource']

print(abe)

# COMMAND ----------

#The inferred schema
infer = spark.createDataFrame([abe])

# COMMAND ----------

#The explicit schema
schema =  fhir_resource_map.schema("Patient")
explicit = spark.createDataFrame([abe], schema) 

# COMMAND ----------


#birth dates match
explicit.select("birthdate").show(truncate=False)

#names all match
explicit.select("name").show(truncate=False)

# COMMAND ----------

print(explicit)

# COMMAND ----------

us_core_fhir_resource_map = FhirSchemaModel(subset="UScore")

# COMMAND ----------

print(us_core_fhir_resource_map.list_keys())

# COMMAND ----------

custom_fhir_resource_map = FhirSchemaModel(us_core_fhir_resource_map.fhir_resource_map)

# COMMAND ----------

print(custom_fhir_resource_map.list_keys())

# COMMAND ----------


