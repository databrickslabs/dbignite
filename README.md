# dbinterop
__Health Data Interoperability__

Utilities to minimize friction in the Databricks
health data Lakehouse.

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
quick exploratory analysis of the people in a FHIR bundle.

> Direct exploratory analysis of 
> FHIR bundles requires complex queries and ETL
> pipelines. In many cases, ad-hoc analysis may not 
> be computationally feasible. _dbinterop_ greatly 
> simplifies "running SQL queries
> on FHIR bundles". 

The _data_model_ module contains a suite of common
health data models such as FHIR, or OMOP CDM. 
_DataModel_ objects can be initialized from other
_DataModel_ objects using a "builder pattern". In this
case we transform the FHIR bundle to a proprietary
"person dashboard" model intended for low friction
exploratory analytics of the people in the bundle.

See: [DataModels](#datamodels)

```
from dbinterop.data_model import FhirBundles, PersonDashboard

person_dashboard = PersonDashboard.builder(from_=FhirBundles(TEST_BUNDLE_PATH))
person_dashboard.summary().display()
```
![image](https://user-images.githubusercontent.com/1669062/150756656-dc7c8d87-b37f-40a7-9177-19fe30d99b0f.png)

![image](https://user-images.githubusercontent.com/1669062/150757341-8f9fd05e-fa1f-458b-b59d-8203f75a49c6.png)

> [See this in a notebook.](demo.py)

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
  integration between upstream data sources and the data lake is out of
  scope. Data is assumed to be landed in the data lake. Though, the
  pattern could in theory be extended to include integration -
  the _Transformer_ can take any arbitrary input to
  find the data.
- FHIR Bundles: dbinterop has support for the convention of data for one patient per bundle
  in a directory. In general, we make no assumptions about what resources will be found in
  a FHIR bundle, or what the relationships between them will be.
- These design principles will need to be elaborated
  on when implementing use cases that require "unstructure" data mapping.
  For example, OMOP source to concept mapping. For the "January project",
  those use cases are out of scope.

# Interop Pipeline Design

## DataModels
_DataModels_ hold the state of an interoperable _DataModel_ 
such as FHIR bundles, or OMOP CDM. The implement a _builder_
pattern to initialize the data model from another
interoperable data model.

[![](https://mermaid.ink/img/eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgRGF0YU1vZGVsIDx8LS0gUGVyc29uRGFzaGJvYXJkXG4gICAgRGF0YU1vZGVsIDx8LS0gT21vcENkbVxuICAgIERhdGFNb2RlbCA8fC0tIEZoaXJCdW5kbGVzXG4gICAgRGF0YU1vZGVsOiArYnVpbGRlcihmcm9tXyBEYXRhTW9kZWwpJFxuICAgIERhdGFNb2RlbDogK3N1bW1hcnkoKSBEYXRhRnJhbWVcbiAgICBEYXRhTW9kZWw6ICtsaXN0RGF0YWJhc2VzKCkgTGlzdH5EYXRhYmFzZX5cblxuICAgIFxuICAgICIsIm1lcm1haWQiOnsidGhlbWUiOiJkZWZhdWx0In0sInVwZGF0ZUVkaXRvciI6ZmFsc2UsImF1dG9TeW5jIjp0cnVlLCJ1cGRhdGVEaWFncmFtIjpmYWxzZX0)](https://mermaid-js.github.io/mermaid-live-editor/edit/#eyJjb2RlIjoiY2xhc3NEaWFncmFtXG4gICAgRGF0YU1vZGVsIDx8LS0gUGVyc29uRGFzaGJvYXJkXG4gICAgRGF0YU1vZGVsIDx8LS0gT21vcENkbVxuICAgIERhdGFNb2RlbCA8fC0tIEZoaXJCdW5kbGVzXG4gICAgRGF0YU1vZGVsOiArYnVpbGRlcihmcm9tXyBEYXRhTW9kZWwpJFxuICAgIERhdGFNb2RlbDogK3N1bW1hcnkoKSBEYXRhRnJhbWVcbiAgICBEYXRhTW9kZWw6ICtsaXN0RGF0YWJhc2VzKCkgTGlzdH5EYXRhYmFzZX5cblxuICAgIFxuICAgICIsIm1lcm1haWQiOiJ7XG4gIFwidGhlbWVcIjogXCJkZWZhdWx0XCJcbn0iLCJ1cGRhdGVFZGl0b3IiOmZhbHNlLCJhdXRvU3luYyI6dHJ1ZSwidXBkYXRlRGlhZ3JhbSI6ZmFsc2V9)


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
omop_cdm.summary().display() # `summary()` can be used for summary statistics or telemetry.
```

## Non-patient centric analytics
The _PersonDashborad DataModel_ is person oriented - every row is a person.
Other dashboard _DataModels_ may have different granularity:
```
# Note: the only change is "person" -> "procedure"
dashboard = dbinterop.transformers.fhir_bundles_to_procedure_dashboard(path_to_my_fhir_bundles)
dashboard.summary().display()
)
```

## Support for additional input data models
The `dbinterop` package can be extended with additional parsers to support
other health data models. For example:
```
omop_cdm = dbinterop.transformers.hl7v2_to_omop_cdm(path_to_my_hl7_messages, ser='xml')
```

