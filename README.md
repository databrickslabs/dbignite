# Databricks-Health-Interop
Health Data Interoperability Project with Databricks

# Summary

# Usage Examples

- All FHIR data is read as FHIR bundles from the disk.
```
import dbinterop

path_to_my_fhir_bundle = 'test_bundle.json'
df = dbinterop.fhir_to_df(path_to_my_fhir_bundle)
```

> Note: By default, `df` is our interpretation of
> a denormalized OMOP "compatible" data model - a
> data model well suited for exploratory analysis of
> patient data. Future extenstions will enable mapping
> to other data models.

The above is equivalent to:
```
dbinterop.fhir_to_df(path_to_my_fhir_bundle, pivot_resouce='Patient')
```
We can also pivot around other resources for quick analysis at
a different granularity:
```
dbinterop.fhir_to_df(path_to_my_fhir_bundle, pivot_resouce='Practitioner')
```

ASUMMPTIONS TO BE VALIDATED:
- No REST API integration, only bundle files.
- Because we are only focused on patient centric exploratory
  analytics in the first phase, we only parse top level patient
  data from bundles. (You can put whatever you want in
  a bundle as long as it has a relationship to a patient.
  We will visualize at the patient granularity).

> TODO: Screenshot of workflow for visualizing DF

# Design Principles
- Data Model Agnostic ("Interoperable")

# Future Steps
- Support for additional input/output data models (in addition to FHIR/OMOP).
- Output dimensional data models, or transactional data models (normalized).
- Non-patient centric analytics.
