# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

pip install git+https://github.com/databricks-industry-solutions/dbignite.git@feature-FHIR-schema-dbignite-HLSSA-289

# COMMAND ----------

from dbignite.fhir_mapping_model import *

# COMMAND ----------

fhir_resource_map = fhirSchemaModel()

# COMMAND ----------

fhir_resource_map.resource("Account")

# COMMAND ----------

fhir_resource_map.list_packaged_data()

# COMMAND ----------

fhir_resource_map.debug_print_keys()

# COMMAND ----------

data = json.load(open("../sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json", "r"))
abe = data['entry'][0]['resource']

print(abe)

# COMMAND ----------

#The inferred schema
infer = spark.createDataFrame([abe])

# COMMAND ----------

#The explicit schema
schema =  fhir_resource_map.resource("Patient")
explicit = spark.createDataFrame([abe], schema) 

display(explicit)

# COMMAND ----------


#birth dates match
explicit.select("birthdate").show(truncate=False)

#names all match
explicit.select("name").show(truncate=False)
