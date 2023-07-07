# Databricks notebook source
# MAGIC %sh 
# MAGIC
# MAGIC mkdir ../schemas

# COMMAND ----------

import json
from pyspark.sql.types import *

for item in dbutils.fs.ls("dbfs:/user/hive/warehouse/json2spark-schema/spark_schemas/"):
    patient_schema = None
    file_location = "/tmp/" + item.name
    dbutils.fs.cp(
        "dbfs:/user/hive/warehouse/json2spark-schema/spark_schemas/" + item.name,
        "file://" + file_location,
    )
    with open(file_location) as new_file:
        patient_schema = json.load(new_file)
    with open("../schemas/" + item.name, "w") as new_file:
        new_file.write(json.dumps(patient_schema, indent = 4) )
    dbutils.fs.rm("./" + item.name)

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC cd ../schemas
# MAGIC ls

# COMMAND ----------


