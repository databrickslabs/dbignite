# 🔥 dbignite
__Health Data Interoperability__

This library is designed to provide a low friction entry to performing analytics on 
[FHIR](https://hl7.org/fhir/bundle.html) bundles by extracting resources and flattening. 

# Usage

## Installation
```
pip install git+https://github.com/databricks-industry-solutions/dbignite.git
```

## Usage: Read & Analyze a FHIR Bundle

This functionality exists in two components 

1. Representation of a FHIR schema and resources (see dbiginte/schemas, dbignite/fhir_mapping_model.py)
2. Interpretation of a FHIR bundle for consumable analtyics (see dbiginte/*py)

### 1. FHIR representations

``` python 
from  dbignite.fhir_mapping_model import FhirSchemaModel
fhir_schema = FhirSchemaModel()

#list all supported FHIR resources
sorted(fhir_schema.list_keys()) # ['Account', 'ActivityDefinition', 'ActorDefinition'...

#only use a subset of FHIR resources (built in CORE list)
fhir_core = FhirSchemaModel().us_core_fhir_resource_mapping()
sorted(fhir_core.list_keys()) # ['AllergyIntolerance', 'CarePlan', 'CareTeam', 'Condition', ...

#create your own custom resource list
fhir_custom = FhirSchemaModel().custom_fhir_resource_mapping(['Patient', 'Claim', 'Condition'])
sorted(fhir_custom.list_keys()) # ['Claim', 'Condition', 'Patient']

#create your own custom schema mapping (advanced usage, not recommended)
# ... FhirSchemaModel(fhir_resource_map = <your dictionary of resource to spark schema>)
```

### 2. FHIR interpretation for analytics

#### Summary Level FHIR Bundle Information

``` python
from pyspark.sql.functions import size,col, sum
from dbignite.readers import read_from_directory

#Set the reader to look at the sample data in this repo
sample_data = "./dbignite-forked/sampledata/*json"
bundle = read_from_directory(sample_data)

#Read all the bundles and parse
bundle.entry

#Show the total number of patient resources in all bundles
bundle.count_resource_type("Patient").show() 
#+------------+                                                                  
#|resource_sum|
#+------------+
#|           3|
#+------------+
#

#Show number of patient resources within each bundle 
bundle.count_within_bundle_resource_type("Patient").show()
#+-------------------+
#|resource_bundle_sum|
#+-------------------+
#|                  1|
#|                  1|
#|                  1|
#+-------------------+

```

#### FHIR Bundle Representation in DBIgnite

The core of a  FHIR bundle is the list of entry resources. This information is flattened into individual columns grouped by resourceType in DBIgnite. The following examples depict common uses and interactions. 

![logo](/img/FhirBundleSchemaClass.png?raw=true)

#### Detailed Mapping Level FHIR Bundle Information (SQL API)

``` python
%python
#Save Claim and Patient data to a table
spark.sql("""DROP TABLE IF EXISTS hls_dev.default.claim""")
spark.sql("""DROP TABLE IF EXISTS hls_dev.default.patient""")

df = bundle.entry.withColumn("bundleUUID", expr("uuid()"))
( df
	.select(col("bundleUUID"), col("Claim"))
	.write.mode("overwrite")
	.saveAsTable("hls_dev.default.claim")
)

( df
	.select(col("bundleUUID"), col("Patient"))
	.write.mode("overwrite")
	.saveAsTable("hls_dev.default.patient")
)
```
``` SQL
%sql
# Select claim line detailed information
select p.bundleUUID as UNIQUE_FHIR_ID, 
  p.Patient.id as patient_id,
  p.patient.birthDate,
  c.claim.patient as claim_patient_id, --Note this column looks unstructed because it is an ambigious "reference" in the FHIR JSON schema. Can be customized  further as well 
  c.claim.id as claim_id,
  c.claim.type.coding.code[0] as claim_type_cd, --837I = Institutional, 837P = Professional
  c.claim.insurance.coverage[0],
  c.claim.total.value as claim_billed_amount,
  c.claim.item.productOrService.coding.display as procedure_description,
  c.claim.item.productOrService.coding.code as procedure_code,
  c.claim.item.productOrService.coding.system as procedure_coding_system
from (select bundleUUID, explode(Patient) as patient from hls_dev.default.patient) p --all patient information
  inner join (select bundleUUID, explode(claim) as claim from hls_dev.default.claim) c --all conditions from that patient 
    on p.bundleUUID = c.bundleUUID --Only show records that were bundled together
limit 100
```

#### Detailed Mapping Level FHIR Bundle Information (DataFrame API)

Perform same functionality above, except using Dataframe only

``` python
df = bundle.entry.withColumn("bundleUUID", expr("uuid()"))

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
)
```

#### Usage: Writing Data as a FHIR Bundle 

>  **Warning** 
> This section is under construction

#### Usage: Seeing a Patient in a Hospital in Real Time  

>  **Warning** 
> This section is under construction

#### Usage: OMOP Common Data Model 

See [DBIgnite OMOP](dbignite/omop) for details 
