# Databricks notebook source
# MAGIC %md # Install DBIgnite for FHIR 

# COMMAND ----------

# DBTITLE 1,Installing DBIgnite
# MAGIC %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

# MAGIC %md # Read in FHIR Data (C-CDA Messages)

# COMMAND ----------

# DBTITLE 1,Read in Sample Data
from  dbignite.fhir_mapping_model import FhirSchemaModel
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import uuid
from dbignite.readers import read_from_directory

sample_data = "s3://hls-eng-data-public/data/synthea/fhir/fhir/*json"

#Read data from a static directory and parse it using entry() function
bundle = read_from_directory(sample_data)
df = bundle.entry()

# COMMAND ----------

# DBTITLE 1,Print Patient Schema
df.select(col("Patient")).printSchema()

# COMMAND ----------

# MAGIC %md # ETL Using Dataframe API
# MAGIC Working with Patient Data and Write Results to Tables
# MAGIC
# MAGIC Note: Synthetic data uses SNOWMED coding system. In Healthcare, ICD10 PCS, ICD CM, CPT4, HCPCS are the accepted codes

# COMMAND ----------

# MAGIC %md ## Conditions

# COMMAND ----------

# DBTITLE 1,Patient Conditions Sample Data
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
).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.patient_conditions")

# COMMAND ----------

# MAGIC %md ## Claims

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
).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.patient_claims")

# COMMAND ----------

# MAGIC %md ## Medications
# MAGIC
# MAGIC Note: The synthetic dataset does not adhere to FHIR standards. In the next cell we extend our schema to support this non-standard structure, medicationCodealeConcept

# COMMAND ----------


med_schema = df.select(explode("MedicationRequest").alias("MedicationRequest")).schema
#Add the medicationCodeableConcept schema in
medCodeableConcept = StructField("medicationCodeableConcept", StructType([
              StructField("text",StringType()),
              StructField("coding", ArrayType(
                StructType([
                    StructField("code", StringType()),
                    StructField("display", StringType()),
                    StructField("system", StringType()),
                ])
              ))
    ]))

med_schema.fields[0].dataType.add(medCodeableConcept) #Add StructField one level below MedicationRequest 

# COMMAND ----------

#reconstruct the schema object with updated Medication schema
old_schemas = {k:v for (k,v) in FhirSchemaModel().fhir_resource_map.items() if k != 'MedicationRequest'}
new_schemas = {**old_schemas, **{'MedicationRequest': med_schema.fields[0].dataType} }

#reread in the data
bundle = read_from_directory(sample_data)
df = bundle.entry(schemas = FhirSchemaModel(fhir_resource_map = new_schemas))

# COMMAND ----------

# DBTITLE 1,Show Medication Requests Data
df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("MedicationRequest")).select(col("Patient"), col("bundleUUID"), explode(col("MedicationRequest")).alias("MedicationRequest")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("MedicationRequest.status"),
  col("MedicationRequest.intent"),
  col("MedicationRequest.authoredOn"),
  col("MedicationRequest.medicationCodeableConcept.text").alias("rx_text"),
  col("MedicationRequest.medicationCodeableConcept.coding.code")[0].alias("rx_code"),
  col("MedicationRequest.medicationCodeableConcept.coding.system")[0].alias("code_type")
).filter(col("Patient").like("efee780e%") |  col("Patient").like("1a5e6090%")).show()

# COMMAND ----------

# DBTITLE 1,Save Medication Requests Data
df.select(explode("Patient").alias("Patient"), col("bundleUUID"), col("MedicationRequest")).select(col("Patient"), col("bundleUUID"), explode(col("MedicationRequest")).alias("MedicationRequest")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("patient.id").alias("Patient"),
  col("MedicationRequest.status"),
  col("MedicationRequest.intent"),
  col("MedicationRequest.authoredOn"),
  col("MedicationRequest.medicationCodeableConcept.text").alias("rx_text"),
  col("MedicationRequest.medicationCodeableConcept.coding.code")[0].alias("rx_code"),
  col("MedicationRequest.medicationCodeableConcept.coding.system")[0].alias("code_type")
).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.medication_requests")

# COMMAND ----------

# MAGIC %md ## Providers

# COMMAND ----------

# DBTITLE 1,Show Provider Data
# Note: providers can be any of (Practitioner, Organization, PractitionerRole)
# For this example we show practitioners

df.select(col("bundleUUID"), col("Practitioner")).select(col("bundleUUID"), explode("Practitioner").alias("Practitioner")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("practitioner.active"),
  col("practitioner.gender"),
  col("practitioner.telecom.system")[0].alias("primary_contact_method"),
  col("practitioner.telecom.value")[0].alias("primary_contact_value"),
  col("practitioner.telecom.use")[0].alias("primary_use")
).show()


# COMMAND ----------

# DBTITLE 1,Save Provider Data
df.select(col("bundleUUID"), col("Practitioner")).select(col("bundleUUID"), explode("Practitioner").alias("Practitioner")).select(
  col("bundleUUID").alias("UNIQUE_FHIR_ID"), 
  col("practitioner.active"),
  col("practitioner.gender"),
  col("practitioner.telecom.system")[0].alias("primary_contact_method"),
  col("practitioner.telecom.value")[0].alias("primary_contact_value"),
  col("practitioner.telecom.use")[0].alias("primary_use")
).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.providers_practitioners")

# COMMAND ----------

# MAGIC %md # ETL Using SQL 
# MAGIC Write FHIR as is to Table and Use SQL to manipulate

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.Patient")
spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.Condition")
spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.Claim")
spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.MedicationRequest")
spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.Practitioner")

bundle.bulk_table_write(location="hls_healthcare.hls_dev" 
  ,write_mode="overwrite"
  ,columns=["Patient", "Condition", "Claim", "MedicationRequest", "Practitioner"]) #if columns is not specified, all resources are written by default

# COMMAND ----------

# MAGIC %md ## Conditions

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
# MAGIC from (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.patient) p --all patient information
# MAGIC   inner join (select bundleUUID, explode(condition) as condition from hls_healthcare.hls_dev.condition) c --all conditions from that patient 
# MAGIC     on p.bundleUUID = c.bundleUUID --Only show records that were bundled together 
# MAGIC

# COMMAND ----------

# MAGIC %md ## Claims

# COMMAND ----------

# DBTITLE 1,Select Claims Information
# MAGIC %sql
# MAGIC select p.bundleUUID as UNIQUE_FHIR_ID, 
# MAGIC   p.Patient.id as patient_id,
# MAGIC   p.patient.birthDate,
# MAGIC   c.claim.patient as claim_patient_id, 
# MAGIC   c.claim.id as claim_id,
# MAGIC   c.claim.type.coding.code[0] as claim_type_cd, --837I = Institutional, 837P = Professional
# MAGIC   c.claim.insurance.coverage[0],
# MAGIC   c.claim.total.value as claim_billed_amount,
# MAGIC   c.claim.item.productOrService.coding.display as procedure_description,
# MAGIC   c.claim.item.productOrService.coding.code as procedure_code,
# MAGIC   c.claim.item.productOrService.coding.system as procedure_coding_system
# MAGIC from (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.patient) p --all patient information
# MAGIC   inner join (select bundleUUID, explode(claim) as claim from hls_healthcare.hls_dev.claim) c --all claim lines from that patient 
# MAGIC     on p.bundleUUID = c.bundleUUID --Only show records that were bundled together 
# MAGIC   limit 10

# COMMAND ----------

# MAGIC %md ## Medications

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.bundleUUID as UNIQUE_FHIR_ID, 
# MAGIC   p.Patient.id as patient_id,
# MAGIC   p.patient.birthDate,
# MAGIC   m.medication.intent,
# MAGIC   m.medication.status,
# MAGIC   m.medication.authoredOn as date_requested,
# MAGIC   m.medication.requester as rx_requester,
# MAGIC   --m.medication.medication --This is where medication should be, but looks like this isn't a compliant FHIR resource. 
# MAGIC                           --Upon further inspection the resource is located at the places below
# MAGIC   
# MAGIC   m.medication.medicationCodeableConcept.coding.code[0] as rx_code,
# MAGIC   m.medication.medicationCodeableConcept.coding.system[0] as rx_code_type,
# MAGIC   m.medication.medicationCodeableConcept.coding.display[0] as rx_description
# MAGIC   from (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.patient) p --all patient information
# MAGIC   inner join (select bundleUUID, explode(MedicationRequest) as medication from hls_healthcare.hls_dev.MedicationRequest) m --all medication orders from that patient 
# MAGIC     on p.bundleUUID = m.bundleUUID --Only show records that were bundled together 
# MAGIC   limit 10

# COMMAND ----------

# MAGIC %md ## Providers

# COMMAND ----------

# DBTITLE 1,Show Provider Contact Information
# MAGIC %sql
# MAGIC select p.bundleUUID as UNIQUE_FHIR_ID,
# MAGIC   p.practitioner.id as provider_id,  --in this FHIR bundle, ID is the FK to other references in various resources (claim, careTeam, etc)
# MAGIC   p.practitioner.active,
# MAGIC   p.practitioner.gender,
# MAGIC   p.practitioner.telecom.system[0] as primary_contact_method,
# MAGIC   p.practitioner.telecom.value[0] as primary_contact_value,
# MAGIC   p.practitioner.telecom.use[0] as primary_use
# MAGIC from (select bundleUUID, explode(practitioner) as practitioner from hls_healthcare.hls_dev.Practitioner) as p
# MAGIC limit 10
# MAGIC

# COMMAND ----------

# DBTITLE 1,Associate Providers to a Claim Resource
# MAGIC %sql
# MAGIC select p.bundleUUID as UNIQUE_FHIR_ID,
# MAGIC   p.practitioner.id as provider_id,  --in this FHIR bundle, ID is the FK to other references in various resources (claim, careTeam, etc)
# MAGIC   p.practitioner.active,
# MAGIC   p.practitioner.gender,
# MAGIC   p.practitioner.telecom.system[0] as primary_contact_method,
# MAGIC   p.practitioner.telecom.value[0] as primary_contact_value,
# MAGIC   p.practitioner.telecom.use[0] as primary_use,
# MAGIC   c.*
# MAGIC from (select bundleUUID, explode(practitioner) as practitioner from hls_healthcare.hls_dev.Practitioner) as p
# MAGIC   inner join  (select claim.id as claim_id, 
# MAGIC                   substring(claim.provider, 82, 36) as provider_id, 
# MAGIC                     claim.type.coding.code[0] as claim_type_cd, --837I = Institutional, 837P = Professional
# MAGIC                     claim.insurance.coverage[0] as insurance,
# MAGIC                     claim.total.value as claim_billed_amount
# MAGIC                   from (select explode(claim) as claim from hls_healthcare.hls_dev.claim)) as c
# MAGIC   on c.provider_id = p.practitioner.id 
# MAGIC   limit 10;
# MAGIC

# COMMAND ----------

# DBTITLE 1,The above returned 0 records for practitioners, why? 
# MAGIC %sql
# MAGIC select claim.type.coding.code[0] as claim_type_cd, --837I = Institutional, 837P = Professional
# MAGIC   count(1)
# MAGIC from (select explode(claim) as claim from hls_healthcare.hls_dev.claim) as c
# MAGIC group by 1 
# MAGIC -- Only institutional and Rx claims present, no professional claims submitted
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md # Deduping FHIR Messages

# COMMAND ----------

# DBTITLE 1,Reread same dataset as above
df = read_from_directory(sample_data).entry()

# COMMAND ----------

# DBTITLE 1,Stage the new data to check for duplicate records
#claim & patient info
df.select(col("bundleUUID"), col("Patient")).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.staging_patient")
df.select(col("bundleUUID"), col("Claim")).write.mode("overwrite").saveAsTable("hls_healthcare.hls_dev.staging_claim")

# COMMAND ----------

# MAGIC %md ## Lookup patient query to dedupe records

# COMMAND ----------

# MAGIC %sql
# MAGIC --Lookup by patient_id 
# MAGIC select stg.bundleUUID as fhir_bundle_id_staging_
# MAGIC   ,p.bundleUUID as fhir_bundle_id_pateint
# MAGIC   ,stg.patient.id as patient_id
# MAGIC   ,case when p.patient.id is not null then "Y" else "N" end as record_exists_flag
# MAGIC from (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.staging_patient) stg
# MAGIC   left outer join (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.patient) p 
# MAGIC     on stg.patient.id = p.patient.id 
# MAGIC limit 20;
# MAGIC

# COMMAND ----------

# MAGIC %md ## Lookup claim query to dedupe records

# COMMAND ----------

# MAGIC %sql
# MAGIC --Lookup by claim_id 
# MAGIC select stg.bundleUUID as fhir_bundle_id_staging_
# MAGIC   ,c.bundleUUID as fhir_bundle_id_pateint
# MAGIC   ,stg.claim.id as claim_id
# MAGIC   ,case when c.claim.id is not null then "Y" else "N" end as record_exists_flag
# MAGIC from  (select bundleUUID, explode(claim) as claim from hls_healthcare.hls_dev.staging_claim) stg
# MAGIC   left outer join (select bundleUUID, explode(claim) as claim from hls_healthcare.hls_dev.claim) c
# MAGIC     on stg.claim.id = c.claim.id 
# MAGIC limit 20;

# COMMAND ----------

# MAGIC %md # Seeing a Patient in Real Time in a Hospital
# MAGIC
# MAGIC Through ADT Feeds

# COMMAND ----------

import os, uuid
from pyspark.sql.functions import *
from dbignite.readers import read_from_directory
from dbignite.hosp_feeds.adt import ADTActions

#Side effect of creating the UDF to see actions from ADT messages 
#SELECT get_action("ADT") -> action : "discharge" , description : "transfer an inpatient to outpatient"
ADTActions()

sample_data = "file:///" + os.getcwd() + "/../sampledata/adt_records/"
bundle = read_from_directory(sample_data)

# COMMAND ----------

# DBTITLE 1,Create tables for Patient and MessageHeader resources
bundle.entry() #must evaluate the DataFrame before writing
spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.Patient")
spark.sql("DROP TABLE IF EXISTS hls_healthcare.hls_dev.MessageHeader")
bundle.bulk_table_write(location="hls_healthcare.hls_dev" 
  ,write_mode="overwrite"
  ,columns=["Patient", "MessageHeader"]) #if columns is not specified, all resources are written by default

# COMMAND ----------

# DBTITLE 1,Query all Patient / Action statuses and their timestamps
# MAGIC %sql
# MAGIC Select 
# MAGIC --SSN value for patient matching
# MAGIC filter(patient.identifier, x -> x.system == 'http://hl7.org/fhir/sid/us-ssn')[0].value as ssn
# MAGIC ,adt.timestamp as event_timestamp
# MAGIC
# MAGIC --ADT action
# MAGIC ,adt.messageheader.eventCoding.code as adt_type
# MAGIC ,get_action(adt.messageheader.eventCoding.code).action as action
# MAGIC ,get_action(adt.messageheader.eventCoding.code).description as description
# MAGIC ,adt.messageheader.eventCoding.code
# MAGIC ,adt.messageheader.eventCoding.system 
# MAGIC
# MAGIC --Patient Resource Details 
# MAGIC ,patient.name[0].given[0] as first_name
# MAGIC ,patient.name[0].family as last_name
# MAGIC ,patient.birthDate
# MAGIC ,patient.gender
# MAGIC --Selecting Driver's license number identifier code='DL'
# MAGIC ,filter(patient.identifier, x -> x.type.coding[0].code == 'DL')[0].value as drivers_license_id
# MAGIC --Master Patient Index Value for patient matching
# MAGIC ,filter(patient.identifier, x -> x.type.text == 'EMPI')[0].value as empi_id
# MAGIC
# MAGIC from (select timestamp, bundleUUID, explode(MessageHeader) as messageheader from hls_healthcare.hls_dev.MessageHeader) adt
# MAGIC   inner join (select bundleUUID, explode(Patient) as patient from hls_healthcare.hls_dev.Patient) patient 
# MAGIC     on patient.bundleUUID = adt.bundleUUID
# MAGIC   order by ssn desc, timestamp desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md # Writing Data into OMOP CDM

# COMMAND ----------

# DBTITLE 1,Transforming data into CDM Person
# MAGIC %sql
# MAGIC
# MAGIC --https://github.com/databrickslabs/dbignite/blob/main/dbignite/omop/utils.py#L14-L42
# MAGIC drop table if exists hls_healthcare.hls_dev.OMOP_PERSON;
# MAGIC create table hls_healthcare.hls_dev.OMOP_PERSON as
# MAGIC SELECT foo.p.id as person_id,
# MAGIC   foo.p.name as name,
# MAGIC   foo.p.gender as gender_source_value,
# MAGIC   year(foo.p.birthDate) as year_of_birth,
# MAGIC   month(foo.p.birthDate) as month_of_birth,
# MAGIC   dayofmonth(foo.p.birthDate) as day_of_birth,
# MAGIC   foo.p.address as patient_address
# MAGIC from 
# MAGIC (SELECT explode(patient) as p
# MAGIC  FROM hls_healthcare.hls_dev.Patient 
# MAGIC ) foo
# MAGIC  ;
# MAGIC

# COMMAND ----------

# MAGIC %md # Writing FHIR Data
# MAGIC
# MAGIC Using CMS SynPUF 
# MAGIC
# MAGIC

# COMMAND ----------

from dbignite.writer.fhir_encoder import *
from dbignite.writer.bundler import *
import json

data = spark.sql("""
select 
--Patient info
b.DESYNPUF_ID, --Patient.id
b.BENE_BIRTH_DT, --Patient.birthDate
b.BENE_COUNTY_CD, --Patient.address.postalCode
c.CLM_ID,  --Claim.id
c.HCPCS_CD_1, --Claim.procedure.procedureCodeableConcept.coding.code
c.HCPCS_CD_2, --Claim.procedure.procedureCodeableConcept.coding.code
c.ICD9_DGNS_CD_1, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
c.ICD9_DGNS_CD_2, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
"http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets" as hcpcs_cdset
from hls_healthcare.hls_cms_synpuf.ben_sum b 
    inner join hls_healthcare.hls_cms_synpuf.car_claims c 
        on c.DESYNPUF_ID = b.DESYNPUF_ID
                 """)

# COMMAND ----------

maps = [Mapping('DESYNPUF_ID', 'Patient.id'), 
		Mapping('BENE_BIRTH_DT', 'Patient.birthDate'),
		Mapping('BENE_COUNTY_CD', 'Patient.address.postalCode'),
    Mapping('CLM_ID', 'Claim.id'),
    Mapping('HCPCS_CD_1', 'Claim.procedure.procedureCodeableConcept.coding.code'),
    Mapping('HCPCS_CD_2', 'Claim.procedure.procedureCodeableConcept.coding.code'),
    #hardcoded values for system of HCPCS
    Mapping('ICD9_DGNS_CD_1', 'Claim.diagnosis.diagnosisCodeableConcept.coding.code'),
    Mapping('ICD9_DGNS_CD_2', 'Claim.diagnosis.diagnosisCodeableConcept.coding.code')
  ]

#For the complex mapping of multiple diagnosis and procedure codes, we override the standard mapping functions 
em = FhirEncoderManager(
  override_encoders ={
    "Claim.procedure.procedureCodeableConcept.coding": 
      FhirEncoder(False, False, lambda x: [{"code": y, "system": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets"} 
            for y in x[0].get("code").split(",")]),
        "Claim.diagnosis.diagnosisCodeableConcept.coding": 
      FhirEncoder(False, False, lambda x: [{"code": y, "system": "http://terminology.hl7.org/CodeSystem/icd9cm"} for y in x[0].get("code").split(",")])  
})
m = MappingManager(maps, data.schema, em)
b = Bundle(m)
result = b.df_to_fhir(data)

# COMMAND ----------

#pretty print 10 values
print('\n'.join([str(x) for x in 
       result.map(lambda x: json.loads(x)).map(lambda x: json.dumps(x, indent=4)).take(10)]))

# COMMAND ----------

# MAGIC %md ## Inspect a single value

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC --Patient info
# MAGIC b.DESYNPUF_ID, --Patient.id
# MAGIC b.BENE_BIRTH_DT, --Patient.birthDate
# MAGIC b.BENE_COUNTY_CD, --Patient.address.postalCode
# MAGIC c.CLM_ID,  --Claim.id
# MAGIC c.HCPCS_CD_1, --Claim.procedure.procedureCodeableConcept.coding.code
# MAGIC c.HCPCS_CD_2, --Claim.procedure.procedureCodeableConcept.coding.code
# MAGIC c.ICD9_DGNS_CD_1, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
# MAGIC c.ICD9_DGNS_CD_2, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
# MAGIC "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets" as hcpcs_cdset
# MAGIC from hls_healthcare.hls_cms_synpuf.ben_sum b 
# MAGIC     inner join hls_healthcare.hls_cms_synpuf.car_claims c 
# MAGIC         on c.DESYNPUF_ID = b.DESYNPUF_ID
# MAGIC where c.CLM_ID = 737363357976870

# COMMAND ----------


data = spark.sql("""
select 
--Patient info
b.DESYNPUF_ID  as DESYNPUF_IDS, --Patient.id
b.BENE_BIRTH_DT, --Patient.birthDate
b.BENE_COUNTY_CD, --Patient.address.postalCode
c.CLM_ID,  --Claim.id
c.HCPCS_CD_1, --Claim.procedure.procedureCodeableConcept.coding.code
c.HCPCS_CD_2, --Claim.procedure.procedureCodeableConcept.coding.code
c.ICD9_DGNS_CD_1, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
c.ICD9_DGNS_CD_2, --Claim.diagnosis.diagnosisCodeableConcept.coding.code
"http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets" as hcpcs_cdset
from hls_healthcare.hls_cms_synpuf.ben_sum b 
    inner join hls_healthcare.hls_cms_synpuf.car_claims c 
        on c.DESYNPUF_ID = b.DESYNPUF_ID
        where c.CLM_ID = 737363357976870
                 """)
                 
m = MappingManager(maps, data.schema, em)
b = Bundle(m)
result = b.df_to_fhir(data)

# COMMAND ----------

#pretty print 10 values
print('\n'.join([str(x) for x in 
       result.map(lambda x: json.loads(x)).map(lambda x: json.dumps(x, indent=4)).take(1)]))

# COMMAND ----------


