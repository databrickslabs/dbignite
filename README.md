# Databricks-Health-Interop
Health Data Interoperability Project with Databricks

# Usage Examples
In this first phase of development, we drive
towards the following core use case. At the same
time we ensure the spec. is [extensible for the future
use cases](#future-extensions).

## Core Use Case: Quick Exploratory Analysis of a FHIR Bundle
The utilities in the _dbinterop_ package can be used
to minimize friction when dealing with a variety of
health data models on the Databricks analytics platform.
In this example, the _dbinterop_ package enables
quick exploratory analysis of the people in FHIR bundle.
The _transformers_ module contains a suite of
"Data Model A" to "Data Model B" transformer classes. In this
case we transform the FHIR bundle to a proprietary
"_dbinterop_ dashboard" model intended for low friction
exploratory analytics on Databricks.

```
import dbinterop

path_to_my_fhir_bundles = '/path/to/json/bundles'
dashboard = dbinterop.transformers.FhirBundleToPersonDbinteropDashboard(path_to_my_fhir_bundles)
dashboard.display()
```
> TODO: Screenshot of workflow for visualizing DF
> Esp. diagnosis by patient

> Note: By default, `df` is our interpretation of
> a denormalized OMOP "compatible" data model - a
> data model well suited for exploratory analysis of
> patient data. Future extenstions will enable mapping
> to other health data models.

# Design Principles
- Data Model Agnostic ("Interoperable"): The goal of this project is to
  provide tools that minimize friction when dealing many health data
  models (of the same data) on the Databricks analytics platform.
- Extensible data model mappings and "intermediate data models".
  See: [Design of the Interop Pipeline](#interop-pipeline-design).
  There is a "many to many" problem when creating mappings between
  different data models. We solve this with the heirarchical combination
  of simple pipeline transforms.
- This package handles interoperability of different data models, but
  integration with upstream data sources and the data lake is out of
  scope. Data is assumed to be landed in the data lake. Though, the
  pattern could in theory be extended to include integration - the
  constructor of the _Transformer_ can take any arbitrary input to
  find the data.
- These design principles will need to be elaborated
  on when implementing use cases that require "unstructure" data mapping.
  For example, OMOP source to concept mapping. For the "January project",
  those use cases are out of scope.

# Interop Pipeline Design

## Transformers
[![](https://mermaid.ink/img/eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgRGlzcGxheWFibGUgPHwtLSBEYXRhTW9kZWxcbiAgICBEYXRhTW9kZWwgPHwtLSBEYmludGVyb3BEYXNoYm9hcmRcbiAgICBEYXRhTW9kZWwgPHwtLSBPbW9wQ2RtXG4gICAgRGF0YU1vZGVsOiArbGlzdERhdGFiYXNlcygpIExpc3R-RGF0YWJhc2V-XG4gICAgRGlzcGxheWFibGU6ICtkaXNwbGF5KCkqXG5cbiAgICBjbGFzcyB0cmFuc2Zvcm1lcn5BLCBCIGV4dGVuZHMgRGF0YU1vZGVsflxuICAgIHRyYW5zZm9ybWVyIDx8Li4gZmhpcl9idW5kbGVfdG9fZGJpbnRlcm9wX2Rhc2hib2FyZFxuICAgIHRyYW5zZm9ybWVyIDx8Li4gZmhpcl9idW5kbGVfdG9fb21vcFxuICAgIHRyYW5zZm9ybWVyIDx8Li4gaGw3djJfdG9fb21vcFxuICAgIHRyYW5zZm9ybWVyOiArX19jYWxsX18oQSBpbnB1dCkqIEJcbiAgICBmaGlyX2J1bmRsZV90b19kYmludGVyb3BfZGFzaGJvYXJkOiArX19jYWxsX18oU3RyaW5nIGJ1bmRsZV9wYXRoKSogRGJpbnRlcmlvRGFzaGJvYXJkXG4gICAgZmhpcl9idW5kbGVfdG9fb21vcDogK19fY2FsbF9fKFN0cmluZyBidW5kbGVfcGF0aCkqIE9tb3BDZG1cbiAgICBobDd2Ml90b19vbW9wOiArX19jYWxsX18oU3RyaW5nIG1lc3NhZ2VfcGF0aCwgU3RyaW5nIHNlcj1cInhtbFwiKSogT21vcENkbVxuICAgIFxuICAgICIsIm1lcm1haWQiOnsidGhlbWUiOiJkZWZhdWx0In0sInVwZGF0ZUVkaXRvciI6ZmFsc2UsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjpmYWxzZX0)](https://mermaid-js.github.io/mermaid-live-editor/edit#eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgRGlzcGxheWFibGUgPHwtLSBEYXRhTW9kZWxcbiAgICBEYXRhTW9kZWwgPHwtLSBEYmludGVyb3BEYXNoYm9hcmRcbiAgICBEYXRhTW9kZWwgPHwtLSBPbW9wQ2RtXG4gICAgRGF0YU1vZGVsOiArbGlzdERhdGFiYXNlcygpIExpc3R-RGF0YWJhc2V-XG4gICAgRGlzcGxheWFibGU6ICtkaXNwbGF5KCkqXG5cbiAgICBjbGFzcyB0cmFuc2Zvcm1lcn5BLCBCIGV4dGVuZHMgRGF0YU1vZGVsflxuICAgIHRyYW5zZm9ybWVyIDx8Li4gZmhpcl9idW5kbGVfdG9fZGJpbnRlcm9wX2Rhc2hib2FyZFxuICAgIHRyYW5zZm9ybWVyIDx8Li4gZmhpcl9idW5kbGVfdG9fb21vcFxuICAgIHRyYW5zZm9ybWVyIDx8Li4gaGw3djJfdG9fb21vcFxuICAgIHRyYW5zZm9ybWVyOiArX19jYWxsX18oQSBpbnB1dCkqIEJcbiAgICBmaGlyX2J1bmRsZV90b19kYmludGVyb3BfZGFzaGJvYXJkOiArX19jYWxsX18oU3RyaW5nIGJ1bmRsZV9wYXRoKSogRGJpbnRlcmlvRGFzaGJvYXJkXG4gICAgZmhpcl9idW5kbGVfdG9fb21vcDogK19fY2FsbF9fKFN0cmluZyBidW5kbGVfcGF0aCkqIE9tb3BDZG1cbiAgICBobDd2Ml90b19vbW9wOiArX19jYWxsX18oU3RyaW5nIG1lc3NhZ2VfcGF0aCwgU3RyaW5nIHNlcj1cInhtbFwiKSogT21vcENkbVxuICAgIFxuICAgICIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOmZhbHNlLCJhdXRvU3luYyI6dHJ1ZSwidXBkYXRlRGlhZ3JhbSI6ZmFsc2V9)
<details>
  <summary>
  </summary>
  
```
classDiagram
    Displayable <|-- DataModel
    DataModel <|-- DbinteropDashboard
    DataModel <|-- OmopCdm
    DataModel: +listDatabases() List~Database~
    Displayable: +display()*

    class transformer~A, B extends DataModel~
    transformer <|.. fhir_bundle_to_dbinterop_dashboard
    transformer <|.. fhir_bundle_to_omop
    transformer <|.. hl7v2_to_omop
    transformer: +__call__(A input)* B
    fhir_bundle_to_dbinterop_dashboard: +__call__(String bundle_path)* DbinterioDashboard
    fhir_bundle_to_omop: +__call__(String bundle_path)* OmopCdm
    hl7v2_to_omop: +__call__(String message_path, String ser="xml")* OmopCdm
```
  
</details>

# Future Extensions

## Support for Dimensional (normalized) or Transactional Output Data Models
For example:
- Proper (dimensional) OMOP.
- Transactional FHIR output.

The basic example above is equivalent to:
```
omop_dfs = dbinterop.parse_fhir_bundles(
    path_to_my_fhir_bundles, 
    mapper=dbinterop.DefaultExploratoryDfMapper()
)
```
The _parser_ handles the input and the _mapper_ handles the output. Parameterize the method call with
a different _mapper_ for a different output data structure. In this example
`omop_dfs` is some sort of collections of DataFrames representing the CDM:
```
omop_cdm = dbinterop.parse_fhir_bundles(path_to_my_fhir_bundles, mapper=dbinterop.OmopMapper())
```

## Non-patient centric analytics
The basic example above is equivalent to:
```
df = dbinterop.parse_fhir_bundles(
    path_to_my_fhir_bundles, 
    mapper=DefaultExploratoryDfMapper(pivot_table='Person')
)
```

We can also pivot around other resources for quick analysis at
a different granularity:
```
df = dbinterop.parse_fhir_bundle(
    path_to_my_fhir_bundles, 
    mapper=DefaultExploratoryDfMapper(pivot_table='Provider')
)
```

## Support for additional input data models
The `dbinterop` package can be extended with additional parsers to support
other health data models. For example:
```
omop_dfs = dbinterop.parse_hl7v2(..., mapper=dbinterop.OMOP_Mapping(...))
```

