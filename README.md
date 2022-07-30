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

### Example usecase:

```
from dbignite.data_model import Transformer
transformer=Transformer(spark)
cdm=transformer.fhir_bundles_to_omop_cdm(BUNDLE_PATH, cdm_database='dbignite_demo')
```

The returned value of the `cdm` is an OmopCDM object with an associated database (`dbignite_demo`), containing the following tables:

- condition
- encounter
- person
- procedure_occurrence

Which are automaically created and added to the specified database (`'dbignite_demo'` in the example above).
As a usecase, one can simply construct cohorts based on these tables and add the cohorts to the same schema or a new schema.

For example you can write `select * from dbignite_demo.person where year_of_birth > 1982 and gender_source_value='male'` to select all male patients who are under 40. 

> [See this in a notebook.](demo.py)

# Interop Pipeline Design

## DataModels
_DataModels_ hold the state of an interoperable _DataModel_ 
such as FHIR bundles, or OMOP CDM. The _Transformer_ class contains 
pre-defined transformations from one data model to another one.
> [see Transformers](#Transformers)
[![](https://mermaid.ink/img/pako:eNptkL0OAjEIx1_lwqRRX6Bx0tNJo4lrF7yi16QfprSD0btnFz8HlYHAnx8EuEATDYGCxiFzbfGY0OtQidWYcS1FV02vk0m1pcQx1MjtPmIyf5mNj6e5-d-_bG2alWAc8VddVSMu3mM6D4YPdSk70C_kLOd7vkcmFnQlef8Reh2eLU8PY_CUPFojx13umobckswFJaGhAxaXNejQCVpOBjMtjM0xgTqgYxoDlhx359CAyqnQG3r96EV1N2GYavQ)](https://mermaid-js.github.io/mermaid-live-editor/edit/#pako:eNptkL0OAjEIx1_lwqRRX6Bx0tNJo4lrF7yi16QfprSD0btnFz8HlYHAnx8EuEATDYGCxiFzbfGY0OtQidWYcS1FV02vk0m1pcQx1MjtPmIyf5mNj6e5-d-_bG2alWAc8VddVSMu3mM6D4YPdSk70C_kLOd7vkcmFnQlef8Reh2eLU8PY_CUPFojx13umobckswFJaGhAxaXNejQCVpOBjMtjM0xgTqgYxoDlhx359CAyqnQG3r96EV1N2GYavQ)

## Transformers
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
the _Transformer_ class contains pre-built pipelines for conversion of different datamodel instances. 
Using methods in _Transformers_ we can transform one datamodel to another.
This pattern also allows simple combination of transformations.

[![](https://mermaid.ink/img/pako:eNp1kcFqwzAMhl9F-NTQ9AXMTlvW29igOQaKGitNwLGCrGyMru8-hzS0dJ1PsvX5Qz86mZodGWtqjzEWHR4F-ypAOqVgiA1LTwJPP5sNbNtOnsfgPMWSX9x_WOqU_EESORQY2wOjuD-ohbVndHsKKh3FvWtWu1SFIwyobQYFKm7TKPToJ-hyXU3cW0rgIfIoNeVwfVGUI2k2G-6Ht-ur5KYHjXCfw3vPQ4JAOQf-JPmSTgkOzD6bZLPyUdBb7SKZlXdgUmcmNylQj51LCzhN0spoSym0sal01ODotTJVOCd0HBwqvbpOWYxt0EfKDY7Ku-9QG6sy0gJd9nihzr-_uKmr)](https://mermaid.live/edit#pako:eNp1kcFqwzAMhl9F-NTQ9AXMTlvW29igOQaKGitNwLGCrGyMru8-hzS0dJ1PsvX5Qz86mZodGWtqjzEWHR4F-ypAOqVgiA1LTwJPP5sNbNtOnsfgPMWSX9x_WOqU_EESORQY2wOjuD-ohbVndHsKKh3FvWtWu1SFIwyobQYFKm7TKPToJ-hyXU3cW0rgIfIoNeVwfVGUI2k2G-6Ht-ur5KYHjXCfw3vPQ4JAOQf-JPmSTgkOzD6bZLPyUdBb7SKZlXdgUmcmNylQj51LCzhN0spoSym0sal01ODotTJVOCd0HBwqvbpOWYxt0EfKDY7Ku-9QG6sy0gJd9nihzr-_uKmr)
