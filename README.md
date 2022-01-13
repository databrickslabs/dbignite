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
dashboard = dbinterop.transformers.fhir_bundles_to_person_dashboard(path_to_my_fhir_bundles)
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
  See: [Design of the Interop Pipelines](#pipelines).
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

## DataModels & Transformers
_Transformers_ are simple functions that output health data
with a target _DataModel_. For example, the _DbinteropPersonDashboard DataModel_
is designed to implement a _display()_ method that simplifies
exploratory analysis of persons.

[![](https://mermaid.ink/img/eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgRGlzcGxheWFibGUgPHwtLSBEYXRhTW9kZWxcbiAgICBEYXRhTW9kZWwgPHwtLSBQZXJzb25EYXNoYm9hcmRcbiAgICBEYXRhTW9kZWwgPHwtLSBPbW9wQ2RtXG4gICAgRGF0YU1vZGVsOiArbGlzdERhdGFiYXNlcygpIExpc3R-RGF0YWJhc2V-XG4gICAgRGlzcGxheWFibGU6ICtkaXNwbGF5KCkqXG5cbiAgICBjbGFzcyB0cmFuc2Zvcm1lcn5BLCBCIGV4dGVuZHMgRGF0YU1vZGVsflxuICAgIHRyYW5zZm9ybWVyIDx8Li4gZmhpcl9idW5kbGVzX3RvX3BlcnNvbl9kYXNoYm9hcmRcbiAgICB0cmFuc2Zvcm1lciA8fC4uIGZoaXJfYnVuZGxlc190b19vbW9wX2NkbVxuICAgIHRyYW5zZm9ybWVyIDx8Li4gb21vcF9jZG1fdG9fcGVyc29uX2Rhc2hib2FyZFxuICAgIHRyYW5zZm9ybWVyOiArX19jYWxsX18oQSBpbnB1dCkqIEJcbiAgICBmaGlyX2J1bmRsZXNfdG9fcGVyc29uX2Rhc2hib2FyZDogK19fY2FsbF9fKFN0cmluZyBidW5kbGVfcGF0aCkqIFBlcnNvbkRhc2hib2FyZFxuICAgIGZoaXJfYnVuZGxlc190b19vbW9wX2NkbTogK19fY2FsbF9fKFN0cmluZyBidW5kbGVfcGF0aCkqIE9tb3BDZG1cbiAgICBvbW9wX2NkbV90b19wZXJzb25fZGFzaGJvYXJkOiArX19jYWxsX18oU3RyaW5nIGNkbV9kYXRhYmFzZSwgU3RyaW5nIG1hcHBpbmdfZGF0YWJhc2UpKiBQZXJzb25EYXNoYm9hcmRcblxuICAgIFxuICAgICIsIm1lcm1haWQiOnsidGhlbWUiOiJkZWZhdWx0In0sInVwZGF0ZUVkaXRvciI6ZmFsc2UsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjpmYWxzZX0)](https://mermaid-js.github.io/mermaid-live-editor/edit#eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgRGlzcGxheWFibGUgPHwtLSBEYXRhTW9kZWxcbiAgICBEYXRhTW9kZWwgPHwtLSBQZXJzb25EYXNoYm9hcmRcbiAgICBEYXRhTW9kZWwgPHwtLSBPbW9wQ2RtXG4gICAgRGF0YU1vZGVsOiArbGlzdERhdGFiYXNlcygpIExpc3R-RGF0YWJhc2V-XG4gICAgRGlzcGxheWFibGU6ICtkaXNwbGF5KCkqXG5cbiAgICBjbGFzcyB0cmFuc2Zvcm1lcn5BLCBCIGV4dGVuZHMgRGF0YU1vZGVsflxuICAgIHRyYW5zZm9ybWVyIDx8Li4gZmhpcl9idW5kbGVzX3RvX3BlcnNvbl9kYXNoYm9hcmRcbiAgICB0cmFuc2Zvcm1lciA8fC4uIGZoaXJfYnVuZGxlc190b19vbW9wX2NkbVxuICAgIHRyYW5zZm9ybWVyIDx8Li4gb21vcF9jZG1fdG9fcGVyc29uX2Rhc2hib2FyZFxuICAgIHRyYW5zZm9ybWVyOiArX19jYWxsX18oQSBpbnB1dCkqIEJcbiAgICBmaGlyX2J1bmRsZXNfdG9fcGVyc29uX2Rhc2hib2FyZDogK19fY2FsbF9fKFN0cmluZyBidW5kbGVfcGF0aCkqIFBlcnNvbkRhc2hib2FyZFxuICAgIGZoaXJfYnVuZGxlc190b19vbW9wX2NkbTogK19fY2FsbF9fKFN0cmluZyBidW5kbGVfcGF0aCkqIE9tb3BDZG1cbiAgICBvbW9wX2NkbV90b19wZXJzb25fZGFzaGJvYXJkOiArX19jYWxsX18oU3RyaW5nIGNkbV9kYXRhYmFzZSwgU3RyaW5nIG1hcHBpbmdfZGF0YWJhc2UpKiBQZXJzb25EYXNoYm9hcmRcblxuICAgIFxuICAgICIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOmZhbHNlLCJhdXRvU3luYyI6dHJ1ZSwidXBkYXRlRGlhZ3JhbSI6ZmFsc2V9)

## Pipelines
One of the key challenges for interoperability of data models is a
"many to many" problem. Support for any new data model requires
cross compatability with many other data models. Intermediate
data models can mitigate this issue.

### The "Many to Many" Problem
[![](https://mermaid.ink/img/eyJjb2RlIjoiZ3JhcGggTFJcbiAgICBBW0FdIC0tPnx0cmFuc2Zvcm18IEIoQilcbiAgICBBW0FdIC0tPnx0cmFuc2Zvcm18IEMoQylcbiAgICBCW0JdIC0tPnx0cmFuc2Zvcm18IEEoQSlcbiAgICBCW0JdIC0tPnx0cmFuc2Zvcm18IEMoQylcbiAgICBDW0NdIC0tPnx0cmFuc2Zvcm18IEEoQSlcbiAgICBDW0NdIC0tPnx0cmFuc2Zvcm18IEIoQilcbiIsIm1lcm1haWQiOnsidGhlbWUiOiJkZWZhdWx0In0sInVwZGF0ZUVkaXRvciI6ZmFsc2UsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjpmYWxzZX0)](https://mermaid.live/edit#eyJjb2RlIjoiZ3JhcGggTFJcbiAgICBBW0FdIC0tPnx0cmFuc2Zvcm18IEIoQilcbiAgICBBW0FdIC0tPnx0cmFuc2Zvcm18IEMoQylcbiAgICBCW0JdIC0tPnx0cmFuc2Zvcm18IEEoQSlcbiAgICBCW0JdIC0tPnx0cmFuc2Zvcm18IEMoQylcbiAgICBDW0NdIC0tPnx0cmFuc2Zvcm18IEEoQSlcbiAgICBDW0NdIC0tPnx0cmFuc2Zvcm18IEIoQilcbiIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOmZhbHNlLCJhdXRvU3luYyI6dHJ1ZSwidXBkYXRlRGlhZ3JhbSI6ZmFsc2V9)

Pipelining existing transforms can significantly simplify
the problem of mapping a variety of data models.
[![](https://mermaid.ink/img/eyJjb2RlIjoiZ3JhcGggTFJcbiAgICBYW0FdIC0tLXx0cmFuc2Zvcm18IEkoSW50ZXJtZWRpYXRlIERhdGFNb2RlbCBYKVxuICAgIFlbQl0gLS0tfHRyYW5zZm9ybXwgSShJbnRlcm1lZGlhdGUgRGF0YU1vZGVsIFgpXG4gICAgWltDXSAtLS18dHJhbnNmb3JtfCBJKEludGVybWVkaWF0ZSBEYXRhTW9kZWwgWClcbiIsIm1lcm1haWQiOnsidGhlbWUiOiJkZWZhdWx0In0sInVwZGF0ZUVkaXRvciI6ZmFsc2UsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjpmYWxzZX0)](https://mermaid.live/edit#eyJjb2RlIjoiZ3JhcGggTFJcbiAgICBYW0FdIC0tLXx0cmFuc2Zvcm18IEkoSW50ZXJtZWRpYXRlIERhdGFNb2RlbCBYKVxuICAgIFlbQl0gLS0tfHRyYW5zZm9ybXwgSShJbnRlcm1lZGlhdGUgRGF0YU1vZGVsIFgpXG4gICAgWltDXSAtLS18dHJhbnNmb3JtfCBJKEludGVybWVkaWF0ZSBEYXRhTW9kZWwgWClcbiIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOmZhbHNlLCJhdXRvU3luYyI6dHJ1ZSwidXBkYXRlRGlhZ3JhbSI6ZmFsc2V9)

### Making Pipelines with Simple Composition
There's no explicit API for pipelining _Transformers_.
Instead, we just use a simple pattern to execute
multiple transforms in series. Here, we use the OMOP CDM
as an intermediate data model, but any intermediate
_DataModel_ can be introduced to simplify interoperability.

[![](https://mermaid.ink/img/eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgZmhpcl9idW5kbGVzX3RvX3BlcnNvbl9kYXNoYm9hcmQgLi4-IGZoaXJfYnVuZGxlc190b19vbW9wX2NkbSA6ICgxKSBjYWxsc1xuICAgIGZoaXJfYnVuZGxlc190b19wZXJzb25fZGFzaGJvYXJkIC4uPiBvbW9wX2NkbV90b19wZXJzb25fZGFzaGJvYXJkIDogKDIpIGNhbGxzXG4gICAgZmhpcl9idW5kbGVzX3RvX3BlcnNvbl9kYXNoYm9hcmQ6ICtfX2NhbGxfXyhTdHJpbmcgYnVuZGxlX3BhdGgpKiBQZXJzb25EYXNoYm9hcmRcbiAgICBmaGlyX2J1bmRsZXNfdG9fb21vcF9jZG06ICtfX2NhbGxfXyhTdHJpbmcgYnVuZGxlX3BhdGgpKiBPbW9wQ2RtXG4gICAgb21vcF9jZG1fdG9fcGVyc29uX2Rhc2hib2FyZDogK19fY2FsbF9fKFN0cmluZyBkYXRhYmFzZSkqIFBlcnNvbkRhc2hib2FyZFxuICAgIFxuICAgICIsIm1lcm1haWQiOnsidGhlbWUiOiJkZWZhdWx0In0sInVwZGF0ZUVkaXRvciI6ZmFsc2UsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjpmYWxzZX0)](https://mermaid.live/edit#eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgZmhpcl9idW5kbGVzX3RvX3BlcnNvbl9kYXNoYm9hcmQgLi4-IGZoaXJfYnVuZGxlc190b19vbW9wX2NkbSA6ICgxKSBjYWxsc1xuICAgIGZoaXJfYnVuZGxlc190b19wZXJzb25fZGFzaGJvYXJkIC4uPiBvbW9wX2NkbV90b19wZXJzb25fZGFzaGJvYXJkIDogKDIpIGNhbGxzXG4gICAgZmhpcl9idW5kbGVzX3RvX3BlcnNvbl9kYXNoYm9hcmQ6ICtfX2NhbGxfXyhTdHJpbmcgYnVuZGxlX3BhdGgpKiBQZXJzb25EYXNoYm9hcmRcbiAgICBmaGlyX2J1bmRsZXNfdG9fb21vcF9jZG06ICtfX2NhbGxfXyhTdHJpbmcgYnVuZGxlX3BhdGgpKiBPbW9wQ2RtXG4gICAgb21vcF9jZG1fdG9fcGVyc29uX2Rhc2hib2FyZDogK19fY2FsbF9fKFN0cmluZyBkYXRhYmFzZSkqIFBlcnNvbkRhc2hib2FyZFxuICAgIFxuICAgICIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOmZhbHNlLCJhdXRvU3luYyI6dHJ1ZSwidXBkYXRlRGlhZ3JhbSI6ZmFsc2V9)

# Future Extensions

## Support for Dimensional (normalized) or Transactional Output Data Models
For example:
- Proper (dimensional) OMOP.
- Transactional FHIR output.

```
omop_cdm = dbinterop.transformers.fhir_bundles_to_omop_cdm(path_to_my_fhir_bundles)
omop_cdm.listDatabases() # Spark DDL is the main interface for something like the CDM.
omop_cdm.display() # `display()` can be used for summary statistics or telemetry.
```

## Non-patient centric analytics
The _PersonDashborad DataModel_ is person oriented - every row is a person.
Other dashboard _DataModels_ may have different granularity:
```
# Note: the only change is "person" -> "procedure"
dashboard = dbinterop.transformers.fhir_bundles_to_procedure_dashboard(path_to_my_fhir_bundles)
dashboard.display()
)
```

## Support for additional input data models
The `dbinterop` package can be extended with additional parsers to support
other health data models. For example:
```
hl7_data = dbinterop.transformers.fhir_bundles_to_(..., mapper=dbinterop.OMOP_Mapping(...))
```

