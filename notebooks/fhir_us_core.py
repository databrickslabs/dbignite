# Databricks notebook source
import json
from pyspark.sql.types import *
from importlib.resources import files

mapping = { str(x).rsplit('/',1)[1][:-5] : StructType.fromJson(json.load(open(x, "r"))) for x in list(files("schemas").iterdir())}

# COMMAND ----------


us_core_resources = ["AllergyIntolerance", "Bundle", "CarePlan", "CareTeam", "Condition", "Coverage", "Device", "DiagnosticReport", "DocumentReference", "Encounter", "Goal", "Immunization", "Location", "Medication", "MedicationDispense", "MedicationRequest", "Observation", "Organization", "Patient", "Practitioner", "Procedure", "Provenance", "Questionnaire", "QuestionnaireResponse", "RelatedPerson", "ServiceRequest", "Specimen"]
    

# COMMAND ----------

us_core_mapping = { x : StructType.fromJson(json.load(open(files('schemas').joinpath(x + ".json"), "r"))) for x in us_core_resources}

# COMMAND ----------

print(us_core_mapping.keys())

# COMMAND ----------

print(us_core_mapping["Patient"])
