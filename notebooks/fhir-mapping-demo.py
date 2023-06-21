# Databricks notebook source
from dbignite.fhir_mapping_model import *

# COMMAND ----------

fhir_resource_map = fhirSchemaModel()

# COMMAND ----------

fhir_resource_map.resource("Account")

# COMMAND ----------

fhir_resource_map.debug_print_keys()

# COMMAND ----------

import json 

with open("../sampledata/Abe_Huels_cec871b4-8fe4-03d1-4318-b51bc279f004.json") as patient_file:
  patient_data = json.load(patient_file)

patient_data["entry"][0]["resource"]

# COMMAND ----------

import os, sys

schema_path = "../schemas"
schema_dir = os.listdir( schemaPath )

resource_mapping = {} 

for file in schema_dir:
  with open("../schemas/" + file) as schema_file:
    schema_struct = StructType.fromJson(json.load(schema_file))
  resource_mapping[file.replace('.json', '')] = schema_struct

   

# COMMAND ----------

resource_mapping["Account"]

# COMMAND ----------

resource_mapping.keys()

# COMMAND ----------

len(resource_mapping)

# COMMAND ----------

resource_mapping["Patient"]

# COMMAND ----------

df_from_dict = spark.read.option("multiline", True).schema(resource_mapping["Patient"]).json(data_string)

display(df_from_dict)


# COMMAND ----------

df_from_dict = spark.read.format("json").option("multiline", True).schema(resource_mapping["Patient"]).load("file:/Workspace/Repos/will.smith@databricks.com/dbignite-FHIR/sampledata/sample_patient_resource.json")

display(df_from_dict)

# COMMAND ----------


