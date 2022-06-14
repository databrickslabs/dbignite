# dbignite
__Health Data Interoperability__

This library is designed to provide a low friction entry to performing analytics on 
[FHIR](https://hl7.org/fhir/bundle.html) bunldes, by extracting patient resources and
writing the data in deltalake. 

# Usage Examples
In this first phase of development, we drive
towards the following core use case. At the same
time we ensure the spec. is [extensible for the future
use cases](#future-extensions).

## Core Use Case: Quick Exploratory Analysis of a FHIR Bundle

The _data_model_ module contains a suite of common
health data models such as FHIR, or OMOP CDM. 

See: [DataModels](#datamodels)

```
from dbignite.data_model import Transformer

cdm_database='dbignite_demo' 
transformer=Transformer(spark)
cdm=transformer.fhir_bundles_to_omop_cdm(BUNDLE_PATH, cdm_database=cdm_database)
```

The returned value of the `cdm` is an OmopCDM object with an associated database (`dbignite_demo`), containing the following tables:

- condition
- encounter
- person
- procedure_occurrence

These tables can be queried using SQL. For example one can simply construct cohorts based on these tables and add the cohorts to the same schema or a new schema.

For example you can write `select * from dbignite_demo.person where year_of_birth > 1982 and gender_source_value='male'` to select all male patients who are under 40. 
