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
bundle.read_entry()

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

#### Detailed Mapping Level FHIR Bundle Information

The core of a  FHIR bundle is the list of entry resources. This information is flattened into individual columns grouped by resourceType in DBIgnite. The following examples depict common uses and interactions. 

![logo](/img/FhirBundleSchemaClass.png?raw=true)

>  **Warning** 
> This section is under construction

#### Usage: Writing Data as a FHIR Bundle 

>  **Warning** 
> This section is under construction

#### Usage: Seeing a Patient in a Hospital in Real Time  

>  **Warning** 
> This section is under construction

#### Usage: OMOP Common Data Model 

>  **Warning** 
> This section has not been updated to reflect latest package updates

See [DBIgnite OMOP](dbignite/omop) for details 
