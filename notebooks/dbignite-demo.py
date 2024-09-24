# Databricks notebook source
# MAGIC %md 
# MAGIC # Analysis of FHIR Bundles using SQL and Python
# MAGIC
# MAGIC <img src="http://hl7.org/fhir/assets/images/fhir-logo-www.png" width = 10%>
# MAGIC
# MAGIC In this demo: 
# MAGIC   1. We use datarbicks `dbignite` package to ingest FHIR bundles (in `json` format) into deltalake
# MAGIC   2. Create a patient-level dashboard from the bundles
# MAGIC   3. Create cohorts
# MAGIC   4. Investigate rate of hospital admissions among covid patients and explore the effect of SDOH and disease history in hospital admissions
# MAGIC   5. Create a feature store of patient features, and use the feature store to create a training dataset for downstream ML workloads, using [databricks feture store](https://docs.databricks.com/applications/machine-learning/feature-store/index.html#databricks-feature-store). 
# MAGIC <br>
# MAGIC </br>
# MAGIC <img src="https://hls-eng-data-public.s3.amazonaws.com/img/FHIR-RA.png" width = 70%>
# MAGIC
# MAGIC ### Data
# MAGIC The data used in this demo is generated using [synthea](https://synthetichealth.github.io/synthea/). We used [covid infections module](https://github.com/synthetichealth/synthea/blob/master/src/main/resources/modules/covid19/infection.json), which incorporates patient risk factors such as diabetes, hypertension and SDOH in determining outcomes. The data is available at `s3://hls-eng-data-public/data/synthea/fhir/fhir/`. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

from dbignite.omop.data_model import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md We have stored simulated FHIR bundles in `s3://hls-eng-data-public/data/synthea/fhir/fhir/` and will directly access those from the notebook.

# COMMAND ----------

BUNDLE_PATH="/home/amir.kermany@databricks.com/health-lakehouse/data"

# COMMAND ----------

# MAGIC %md
# MAGIC Before we start, let's take a look at the files:

# COMMAND ----------

files=dbutils.fs.ls(BUNDLE_PATH)
n_bundles=len(files)

# COMMAND ----------

# DBTITLE 1,list FHIR bundles
files=dbutils.fs.ls(BUNDLE_PATH)
data_size=spark.createDataFrame(files).select(F.sum('size').alias('sum')).collect()[0].sum//1e9
n_bundles=len(files)
displayHTML(f'There are <b>{n_bundles}</b> with a a total size of <b>{data_size}</b> Gb')
display(files,10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load FHIR bundles

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at one of the files

# COMMAND ----------

sample_bundle=dbutils.fs.ls(BUNDLE_PATH)[0].path
print(dbutils.fs.head(sample_bundle,500))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract resources from the bundles
# MAGIC The`fhir_bundles_to_omop_cdm` method extracts resources from the FHIR bunldes and creates: `person`, `condition`, `procedure_occurrence` and `encounters` in the `cdm_database`

# COMMAND ----------

# DBTITLE 1,define database 
cdm_database='dbignite_demo' 
sql(f'DROP SCHEMA IF EXISTS {cdm_database} CASCADE;')

# COMMAND ----------

# DBTITLE 1,define fhir and cdm models
fhir_model=FhirBundles(path=TEST_BUNDLE_PATH)
cdm_model=OmopCdm(cdm_database)

# COMMAND ----------

FhirBundlesToCdm().transform(fhir_model, cdm_model, True)

# COMMAND ----------

# DBTITLE 1,Transform from CDM to a patient dashboard 
cdm2dash_transformer=CdmToPersonDashboard()
dash_model=PersonDashboard()
cdm2dash_transformer.transform(cdm_model,dash_model)
person_dashboard_df = dash_model.summary()

# COMMAND ----------

# DBTITLE 1,display all conditions for a given person
from pyspark.sql import functions as F
display(
   person_dashboard_df
  .filter("person_id='6efa4cd6-923d-2b91-272a-0f5c78a637b8'")
  .select(F.explode('conditions').alias('conditions'))
  .selectExpr('conditions.condition_start_datetime','conditions.condition_start_datetime','conditions.condition_status')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take a look at dbsql tables that we just created in `dbignite_demo` schema

# COMMAND ----------

# DBTITLE 1,list all tables
sql(f'use {cdm_model.listDatabases()[0]}')
display(sql('show tables'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Data Analysis
# MAGIC Now let's take a quick look at the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Person Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from person where year_of_birth > 1982
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Condition Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from condition 

# COMMAND ----------

# DBTITLE 1,Top 20 common conditions
# MAGIC %sql
# MAGIC select condition_code, condition_status, count(*) as counts
# MAGIC from condition
# MAGIC group by 1,2
# MAGIC order by 3 desc
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,age distribution among covid-19 patients
# MAGIC %sql
# MAGIC select 10*floor((year(CURRENT_DATE) - year_of_birth)/10) as age_group, count(*) as count from person p
# MAGIC join condition c on p.person_id=c.person_id
# MAGIC where c.condition_code=840539006
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### procedure_occurrence Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from procedure_occurrence
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,to 10 most common procedures
# MAGIC %sql
# MAGIC select procedure_code,procedure_code_display, count(*) as count from procedure_occurrence
# MAGIC group by 1,2
# MAGIC order by 3 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Encounter Table

# COMMAND ----------

# DBTITLE 1,Encounters
# MAGIC %sql
# MAGIC select * from encounter
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,top 10 encounters
# MAGIC %sql
# MAGIC select encounter_status, count(*) as count
# MAGIC from encounter
# MAGIC group by 1
# MAGIC order by 2 desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## License
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|

# COMMAND ----------

# MAGIC %md
# MAGIC ### Disclaimers
# MAGIC Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account.  Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.
