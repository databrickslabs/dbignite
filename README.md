# Databricks-Health-Interop
Health Data Interoperability Project with Databricks

# Summary

# Usage Examples
In this first phase of development, we drive
towards the following core use case. At the same
time we ensure the spec. is [extensible for the future
use cases](#future-extensions).

### Core Use Case: Quick Exploratory Analysis of a FHIR Bundle

NOTES:
- this should probably be a directory of bundles
- 1 bundle ~= 1 patient
- bundles are a "snapshot in time"
```
import dbinterop

path_to_my_fhir_bundle = '/test_bundle.json'
df = dbinterop.parse_fhir_bundle(path_to_my_fhir_bundle)
```
> TODO: Screenshot of workflow for visualizing DF
> Esp. diagnosis by patient

> Note: By default, `df` is our interpretation of
> a denormalized OMOP "compatible" data model - a
> data model well suited for exploratory analysis of
> patient data. Future extenstions will enable mapping
> to other health data models.

# Design Principles
- Data Model Agnostic ("Interoperable")
- This package handles interoperability of different data models, but
  integration with upstream data sources and the data lake is out of
  scope. Data is assumed to be landed in the data lake.

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
    mapper=DefaultExploratoryDfMapper(pivot_table='Person')
)
```

We can also pivot around other resources for quick analysis at
a different granularity:
```
df = dbinterop.parse_fhir_bundle(
    path_to_my_fhir_bundle, 
    mapper=DefaultExploratoryDfMapper(pivot_table='Provider')
)
```

### Support for additional input data models
The `dbinterop` package can be extended with additional parsers to support
other health data models. For example:
```
omop_dfs = dbinterop.parse_hl7v2(..., mapper=dbinterop.OMOP_Mapping(...))
```

