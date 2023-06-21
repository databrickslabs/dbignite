# Databricks notebook source
# import dbignite.fhir_mapping_model

# COMMAND ----------

# DBTITLE 1,Cannot Import Currently So Copied Class 
import os, sys, json
from pyspark.sql.types import *

class fhirSchemaModel():
    def __init__(self, mapping = None):
        self.mapping = self.createFHIRMapping()

    def createFHIRMapping(self) -> dict[str, str]:
      schema_path = "../schemas"
      schema_dir = os.listdir( schema_path )

      resource_map = {}

      for file in schema_dir:
        with open("../schemas/" + file) as schema_file:
          schema_struct = StructType.fromJson(json.load(schema_file))
          resource_map[file.replace('.json', '')] = schema_struct
      return resource_map
    
    def resource(self, resourceName: str) -> str:
      return self.mapping[resourceName]
    
    # Adding due to ambiguity on resource / schema retrieval call 
    def schema(self, resourceName: str) -> str:
      return self.mapping[resourceName]

    #debugging    
    def debug_print_keys(self):
      print(self.mapping.keys())

# COMMAND ----------

fhir_resource_map = fhirSchemaModel()

# COMMAND ----------

fhir_resource_map.resource("Account")

# COMMAND ----------

fhir_resource_map.debug_print_keys()

# COMMAND ----------

import json 

with open("../sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json") as patient_file:
  patient_data = json.load(patient_file)

patient_data["entry"][0]["resource"]

# COMMAND ----------

## Researching Issue with reading in patient info
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

# COMMAND ----------


