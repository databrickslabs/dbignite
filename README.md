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

### (1) FHIR representations

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

### (2) FHIR interpretation for analytics

## Usage: Writing Data as a FHIR Bundle

## Usage: OMOP Common Data Model 

 **Warning** 
 This section has not been updated to reflect latest package updates

The _data_model_ module contains a suite of common
health data models such as FHIR, or OMOP CDM. 

See: [DataModels](#datamodels)

### Example:
Specify the name of the schema:
```
cdm_database='dbignite_demo' 
```
create data model objects:
```
fhir_model=FhirBundles(BUNDLE_PATH)
cdm_model=OmopCdm(cdm_database)
```
create a transformer:
```
fhir2omop_transformer=FhirBundlesToCdm(spark)
```
transform from FHIR to your CDM:
```
fhir2omop_transformer.transform(fhir_model,cdm_model)
```

The returned value of the `cdm` is an OmopCDM object with an associated database (`dbignite_demo`), containing the following tables:

- condition
- encounter
- person
- procedure_occurrence

Which are automaically created and added to the specified schema (`'dbignite_demo'` in the example above).
As a usecase, one can simply construct cohorts based on these tables and add the cohorts to the same schema or a new schema:
For example to select all male patients born before 1982:

`select * from dbignite_demo.person where year_of_birth < 1982 and gender_source_value='male'` 

> [See this in a notebook.](notebooks/dbignite-demo.py)

# Interop Pipeline Design

## DataModels
_DataModels_ hold the state of an interoperable _DataModel_ 
such as FHIR bundles, or OMOP CDM. The _Transformer_ class contains 
pre-defined transformations from one data model to another one.
> [see Transformers](#Transformers)

```mermaid
classDiagram
    DataModel <|-- PersonDashboard
    DataModel <|-- OmopCdm
    DataModel <|-- FhirBundles
    DataModel: +summary() DataFrame
    DataModel: +listDatabases() List~Databases~
```

## Transformers
One of the key challenges for interoperability of data models is a
"many to many" problem. Support for any new data model requires
cross compatability with many other data models. Intermediate
data models can mitigate this issue.

### The "Many to Many" Problem

```mermaid
graph LR
    A[A] -->|transform| B(B)
    A[A] -->|transform| C(C)
    B[B] -->|transform| A(A)
    B[B] -->|transform| C(C)
    C[C] -->|transform| A(A)
    C[C] -->|transform| B(B)
```

Pipelining existing transforms can significantly simplify
the problem of mapping a variety of data models.

```mermaid
graph LR
    X[A] ---|transform| I(Intermediate DataModel X)
    Y[B] ---|transform| I(Intermediate DataModel X)
    Z[C] ---|transform| I(Intermediate DataModel X)
```

### Making Pipelines with Simple Composition
the _Transformer_ class contains pre-built pipelines for conversion of different datamodel instances. 
Using methods in _Transformers_ we can transform one datamodel to another.
This pattern also allows simple combination of transformations.

```mermaid
classDiagram
    Transformer <|-- FhirBundlesToCdm
    Transformer <|-- CdmToPersonDashboard
    Transformer: +load_entries_df(String path) DataFrame
    Transformer: + transform(DataModel source, DataModel target)
    FhirBundlesToCdm:+transform(FhirBundles from, OmopCdm to, overwrite bool)    
    CdmToPersonDashboard:+transform(OmopCdm from, PersonDashboard to)
```
