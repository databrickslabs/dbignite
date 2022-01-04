# Databricks-Health-Interop
Health Data Interoperability Project with Databricks

# Summary

# Usage Examples
In this first phase of development, we drive
towards the following core use case. At the same
time we ensure the spec. is [extensible for the future
use cases](#future-extensions).

### Core Use Case: Quick Exploratory Analysis of a FHIR Bundle
```
import dbinterop

path_to_my_fhir_bundle = '/test_bundle.json'
df = dbinterop.parse_fhir_bundle(path_to_my_fhir_bundle)
```
> TODO: Screenshot of workflow for visualizing DF

> Note: By default, `df` is our interpretation of
> a denormalized OMOP "compatible" data model - a
> data model well suited for exploratory analysis of
> patient data. Future extenstions will enable mapping
> to other health data models.

**ASUMMPTIONS TO BE VALIDATED**:
- No REST API integration, only bundle files.
- Because we are only focused on patient centric exploratory
  analytics in the first phase, we only parse top level patient
  data from bundles. (You can put whatever you want in
  a bundle as long as it has a relationship to a patient.
  We will visualize at the patient granularity).
- For the first phase, we only output acutomozed, denormalized 
  version of the OMOP data model.

# Design Principles
- Data Model Agnostic ("Interoperable")

# Future Extensions

### Support for Dimensional (normalized) or Transactional Output Data Models
For example:
- Proper (dimensional) OMOP.
- Transactional FHIR output.

The basic example above is equivalent to:
```
omop_dfs = dbinterop.parse_fhir_bundle(
    path_to_my_fhir_bundle, 
    mapper=dbinterop.DefaultExploratoryDfMapper()
)
```
The _parser_ handles the input and the _mapper_ handles the output. Parameterize the method call with
a different _mapper_ for a different output data structure. In this example
`omop_dfs` is some sort of collections of DataFrames representing the CDM:
```
omop_cdm = dbinterop.parse_fhir_bundle(path_to_my_fhir_bundle, mapper=dbinterop.OmopMapper())
```

### Non-patient centric analytics
The basic example above is equivalent to:
```
df = dbinterop.parse_fhir_bundle(
    path_to_my_fhir_bundle, 
    mapper=DefaultExploratoryDfMapper(pivot_resouce='Patient')
)
```

We can also pivot around other resources for quick analysis at
a different granularity:
```
df = dbinterop.parse_fhir_bundle(
    path_to_my_fhir_bundle, 
    mapper=DefaultExploratoryDfMapper(pivot_resouce='Provider')
)
```

### Support for additional input data models
The `dbinterop` package can be extended with additional parsers to support
other health data models. For example:
```
omop_dfs = dbinterop.parse_hl7v2(..., mapper=dbinterop.OMOP_Mapping(...))
```

