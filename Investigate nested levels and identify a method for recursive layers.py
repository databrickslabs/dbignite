# Databricks notebook source
# MAGIC %md
# MAGIC ###Version 1

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel
from dbignite.readers import read_from_directory
from dbignite.readers import FhirFormat

fhir_schema = FhirSchemaModel()
patient_schema = fhir_schema.schema("Patient")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DataType, ArrayType, MapType
)
from typing import Any, List, Dict, get_type_hints

def get_python_type(data_type: DataType) -> Any:
    if isinstance(data_type, IntegerType):
        return int
    elif isinstance(data_type, StringType):
        return str
    elif isinstance(data_type, StructType):
        return dict
    elif isinstance(data_type, ArrayType):
        return List[get_python_type(data_type.elementType)]
    elif isinstance(data_type, MapType):
        return Dict[str, get_python_type(data_type.valueType)]
    else:
        return Any

def flatten_schema(schema: StructType, prefix: str = '') -> List[tuple]:
    fields = []
    for field in schema.fields:
        field_name = f"{prefix}{field.name}" if prefix else field.name
        if isinstance(field.dataType, StructType):
            fields.extend(flatten_schema(field.dataType, f"{field_name}_"))
        else:
            fields.append((field_name, get_python_type(field.dataType)))
    return fields

def generate_class_from_schema(schema: StructType, class_name: str) -> type:
    # Flatten the schema
    flat_fields = flatten_schema(schema)

    # Define the __init__ method
    def __init__(self, **kwargs):
        for field_name, field_type in flat_fields:
            value = kwargs.get(field_name)
            if value is not None:
                if isinstance(field_type, list):
                    element_type = field_type[0]
                    value = [element_type(v) for v in value]
                elif isinstance(field_type, dict):
                    value = {k: str(v) for k, v in value.items()}  # Convert all values to strings
              
            setattr(self, field_name, value)

    # Define the __repr__ method
    def __repr__(self):
        field_values = ", ".join([f"{field_name}={{self.{field_name}}}" for field_name, _ in flat_fields])
        return f"{class_name}({field_values})".format(self=self)

    # Create the class dictionary
    class_dict = {
        '__init__': __init__,
        '__repr__': __repr__,
    }

    return type(class_name, (object,), class_dict)

# Example usage
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", IntegerType(), True)
    ]), True),
    StructField("emails", ArrayType(StringType()), True),
    StructField("phone_numbers", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("number", StringType(), True)
    ])), True),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

# Generate class from schema
Person = generate_class_from_schema(schema, "Person")

# COMMAND ----------

# Sample data
sample_data = {
    "id": 1,
    "name": "John",
    "address_city": "New York",
    "address_state": "NY",
    "address_zip_code": 10001,
    "emails": ["john@example.com", "john.doe@example.com"],
    "phone_numbers": [
        {"type": "home", "number": "123-456-7890"},
        {"type": "work", "number": "987-654-3210"}
    ]
}

# Create an instance of Person class
person = Person(**sample_data)

# Print the person object
print(person)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Version 2

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, IntegerType, DataType, StringType, ArrayType, MapType
from typing import Any, List, Dict

def get_python_type(data_type: DataType) -> Any:
    if isinstance(data_type, IntegerType):
        return int
    elif isinstance(data_type, StringType):
        return str
    elif isinstance(data_type, StructType):
        return dict
    elif isinstance(data_type, ArrayType):
        return List[get_python_type(data_type.elementType)]
    elif isinstance(data_type, MapType):
        return Dict[str, get_python_type(data_type.valueType)]
    else:
        return Any

def flatten_data(data, parent_key='', sep='_'):
    items = []
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_data(v, new_key, sep).items())
            elif isinstance(v, list):
                list_items = []
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        list_items.extend(flatten_data(item, f"{new_key}_{i}", sep).items())
                    else:
                        list_items.append((f"{new_key}_{i}", item))
                if list_items:
                    items.extend(list_items)
                else:
                    items.append((new_key, []))
            else:
                items.append((new_key, v))
    elif isinstance(data, list):
        # Handle lists at the root level if necessary
        for i, item in enumerate(data):
            items.extend(flatten_data(item, f"{parent_key}_{i}", sep).items())
    return dict(items)

def filter_none_and_empty(data):
    if isinstance(data, dict):
        return {k: filter_none_and_empty(v) for k, v in data.items() if v not in [None, [], {}]}
    elif isinstance(data, list):
        return [filter_none_and_empty(item) for item in data if item not in [None, [], {}]]
    return data

class Patient:
    def __init__(self, data):
        for key, value in data.items():
            setattr(self, key, value)

    def __repr__(self):
        attrs = [f"{key}={getattr(self, key)!r}" for key in self.__dict__]
        return f"{self.__class__.__name__}({', '.join(attrs)})"

# Load the JSON data
with open('path/your/json/file.json', 'r') as file:
    json_data = json.load(file)

# Assuming the JSON structure contains an array of entries, which is typical in a FHIR Bundle
# Extract patient data from the JSON
patient_data = None
for entry in json_data.get("entry", []):
    if entry.get("resource", {}).get("resourceType") == "Patient":
        patient_data = entry["resource"]
        break

if patient_data:
    # Clean and flatten the patient data
    cleaned_data = filter_none_and_empty(patient_data)
    flattened_patient_data = flatten_data(cleaned_data)

    # Create an instance of the dynamic class with the flattened patient data
    patient = Patient(flattened_patient_data)

    # Print the patient instance
    print(patient)
else:
    print("No patient data found.")


# COMMAND ----------

import json

def flatten_data(data, parent_key='', sep='_'):
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_data(v, new_key, sep))
            else:
                items[new_key] = v
    elif isinstance(data, list):
        for index, element in enumerate(data):
            new_key = f"{parent_key}{sep}{index}"
            if isinstance(element, (dict, list)):
                items.update(flatten_data(element, new_key, sep))
            else:
                items[new_key] = element
    return items

class PatientData:
    def __init__(self, data):
        for key, value in data.items():
            setattr(self, key, value)

    def __repr__(self):
        attrs = [f"{key}={getattr(self, key)!r}" for key in self.__dict__]
        return f"{self.__class__.__name__}({', '.join(attrs)})"

class CondensedPatientDetails:
    def __init__(self, patient_data):
        self.patient_data = patient_data

    def __str__(self):
        details = (f"Condensed Patient Details:\n"
                   f"Patient: {getattr(self.patient_data, 'name_0_family', 'Unknown')} {getattr(self.patient_data, 'name_0_given_0', 'Unknown')}\n"
                   f"Gender: {getattr(self.patient_data, 'gender', 'Unknown')}\n"
                   f"Birth Date: {getattr(self.patient_data, 'birthDate', 'Unknown')}\n"
                   f"Address: {getattr(self.patient_data, 'address_0_line_0', 'Unknown')}, {getattr(self.patient_data, 'address_0_city', 'Unknown')}, "
                   f"{getattr(self.patient_data, 'address_0_state', 'Unknown')}, {getattr(self.patient_data, 'address_0_postalCode', 'Unknown')}, {getattr(self.patient_data, 'address_0_country', 'Unknown')}\n"
                   f"Relationship Status: {getattr(self.patient_data, 'maritalStatus_coding_0_display', 'Unknown')}\n"
                   f"Primary Language: {getattr(self.patient_data, 'communication_0_language_text', 'Unknown')}\n"
                   f"Contact Phone: {getattr(self.patient_data, 'telecom_0_value', 'Unknown')}\n"
                   f"Driver's License: {getattr(self.patient_data, 'identifier_3_value', 'N/A')}\n"
                   f"Passport Number: {getattr(self.patient_data, 'identifier_4_value', 'N/A')}\n"
                   f"Medical Record Number: {getattr(self.patient_data, 'identifier_1_value', 'N/A')}\n"
                   f"Social Security Number: {getattr(self.patient_data, 'identifier_2_value', 'N/A')}\n"
                   f"Mother's Maiden Name: {getattr(self.patient_data, 'extension_2_valueString', 'N/A')}\n"
                   f"Race: {getattr(self.patient_data, 'extension_0_extension_0_valueCoding_display', 'N/A')}\n"
                   f"Ethnicity: {getattr(self.patient_data, 'extension_1_extension_0_valueCoding_display', 'N/A')}\n"
                   
        )
        return details

# Load the JSON data
with open('path/your/json/file.json', 'r') as file:
    json_data = json.load(file)

# Assuming the JSON structure contains an array of entries, typical in a FHIR Bundle
patient_data = None
for entry in json_data.get("entry", []):
    if entry.get("resource", {}).get("resourceType") == "Patient":
        patient_data = entry["resource"]
        break

if patient_data:
    # Clean and flatten the patient data
    cleaned_data = flatten_data(patient_data) 

    # Create an instance of PatientData with the flattened patient data
    patient_data_instance = PatientData(cleaned_data)

    # Create an instance of CondensedPatientDetails
    condensed_details = CondensedPatientDetails(patient_data_instance)
    print(condensed_details)
else:
    print("No patient data found.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Version 3

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, DataType, MapType
from typing import Any, List, Dict

def get_python_type(data_type: DataType) -> Any:
    if isinstance(data_type, IntegerType):
        return int
    elif isinstance(data_type, StringType):
        return str
    elif isinstance(data_type, StructType):
        return dict
    elif isinstance(data_type, ArrayType):
        return List[get_python_type(data_type.elementType)]
    elif isinstance(data_type, MapType):
        return Dict[str, get_python_type(data_type.valueType)]
    else:
        return Any
    
def flatten_data(data, parent_key='', sep='_'):
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_data(v, new_key, sep))
            else:
                items[new_key] = v
    elif isinstance(data, list):
        for index, element in enumerate(data):
            new_key = f"{parent_key}{sep}{index}"
            if isinstance(element, (dict, list)):
                items.update(flatten_data(element, new_key, sep))
            else:
                items[new_key] = element
    return items

class PatientData:
    def __init__(self, data):
        for key, value in data.items():
            setattr(self, key, value)

    def __repr__(self):
        attrs = [f"{key}={getattr(self, key)!r}" for key in self.__dict__]
        return f"{self.__class__.__name__}({', '.join(attrs)})"

class CondensedPatientDetails:
    def __init__(self, patient_data):
        self.patient_data = patient_data

    def __str__(self):
        # Debug: Print all available keys and values
        print(self.patient_data.__dict__)
        
       
        details = (f"Condensed Patient Details:\n"
                   f"Patient: {getattr(self.patient_data, 'name_0_family', 'Unknown')} {getattr(self.patient_data, 'name_0_given_0', 'Unknown')}\n"
                   f"Gender: {getattr(self.patient_data, 'gender', 'Unknown')}\n"
                   f"Birth Date: {getattr(self.patient_data, 'birthDate', 'Unknown')}\n"
                   f"Address: {getattr(self.patient_data, 'address_0_line_0', 'Unknown')}, {getattr(self.patient_data, 'address_0_city', 'Unknown')}, "
                   f"{getattr(self.patient_data, 'address_0_state', 'Unknown')}, {getattr(self.patient_data, 'address_0_postalCode', 'Unknown')}, {getattr(self.patient_data, 'address_0_country', 'Unknown')}\n"
                   f"Relationship Status: {getattr(self.patient_data, 'maritalStatus_coding_0_display', 'Unknown')}\n"
                   f"Primary Language: {getattr(self.patient_data, 'communication_0_language_text', 'Unknown')}\n"
                   f"Contact Phone: {getattr(self.patient_data, 'telecom_0_value', 'Unknown')}\n"
                   f"Driver's License: {getattr(self.patient_data, 'identifier_3_value', 'N/A')}\n"
                   f"Passport Number: {getattr(self.patient_data, 'identifier_4_value', 'N/A')}\n"
                   f"Medical Record Number: {getattr(self.patient_data, 'identifier_1_value', 'N/A')}\n"
                   f"Social Security Number: {getattr(self.patient_data, 'identifier_2_value', 'N/A')}\n"
                   f"Mother's Maiden Name: {getattr(self.patient_data, 'extension_2_valueString', 'N/A')}\n"
                   f"Race: {getattr(self.patient_data, 'extension_0_extension_0_valueCoding_display', 'N/A')}\n"
                   f"Ethnicity: {getattr(self.patient_data, 'extension_1_extension_0_valueCoding_display', 'N/A')}\n"
                   
        )
        return details

# Load and process the JSON data as previously shown
# Insert the same JSON loading and processing logic here

if patient_data:
    # Clean and flatten the patient data
    cleaned_data = flatten_data(patient_data)

    # Print to debug
    print("Patient:", cleaned_data)

    # Create an instance of PatientData with the flattened patient data
    patient_data_instance = PatientData(cleaned_data)

    # Create an instance of CondensedPatientDetails
    condensed_details = CondensedPatientDetails(patient_data_instance)
    print(condensed_details)
else:
    print("No patient data found.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## the last version this is only to change the ResouceType: like Patient , Encounter , Claim Etc..
# MAGIC

# COMMAND ----------

import json

def parse_json_recursively(json_object, prefix=''):
    """ Recursively parse JSON object into flat dictionary with compound keys."""
    if isinstance(json_object, dict):
        for key, value in json_object.items():
            temp_prefix = f"{prefix}{key}_" if prefix else f"{key}_"
            for result in parse_json_recursively(value, temp_prefix):
                yield result
    elif isinstance(json_object, list):
        for index, item in enumerate(json_object):
            temp_prefix = f"{prefix}{index}_"
            for result in parse_json_recursively(item, temp_prefix):
                yield result
    else:
        yield prefix[:-1], json_object

def find_resource_by_type(data, resource_type):
    """ Find dictionary entries by resourceType within nested structures."""
    if isinstance(data, dict):
        if data.get('resourceType', '') == resource_type:
            return data
        else:
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    found = find_resource_by_type(value, resource_type)
                    if found:
                        return found
    elif isinstance(data, list):
        for item in data:
            found = find_resource_by_type(item, resource_type)
            if found:
                return found
    return None

def generate_class_from_data(data, class_name="Patient"):
    """ Generate a class from the parsed data and define its __repr__ method for detailed output."""
    attributes = dict(parse_json_recursively(data))
    class_body = "\n".join(f"        self.{k} = kwargs.get('{k}', {repr(v)})" for k, v in attributes.items())
    repr_body = ", ".join(f"{k}={{self.{k}!r}}" for k in attributes.keys())
    class_definition = f"""
class {class_name}:
    def __init__(self, **kwargs):
{class_body}

    def __repr__(self):
        return f"{class_name}({repr_body})"
"""
    return class_definition, attributes

# Load JSON data
with open('/path/to/your/json_file.json', 'r') as file:
    json_data = json.load(file)

# Find the specific resource type
resource_type = 'Patient'
patient_data = find_resource_by_type(json_data, resource_type)

if patient_data:
    patient_class_code, patient_attrs = generate_class_from_data(patient_data, resource_type)
    exec(patient_class_code)
    patient_instance = eval(resource_type)(**patient_attrs)
    print(patient_instance)
else:
    print("No patient data found in the JSON file.")


# COMMAND ----------

import json

def extract_from_bundle(json_data, resource_type):
    """Extracts resources of a specific type from a FHIR Bundle or standalone resource."""
    resources = []
    if json_data.get('resourceType') == 'Bundle':
        for entry in json_data.get('entry', []):
            resource = entry.get('resource')
            if resource and resource.get('resourceType') == resource_type:
                resources.append(resource)
    elif json_data.get('resourceType') == resource_type:
        resources.append(json_data)
    return resources

def parse_resource(resource):
    """Parses a single FHIR resource to extract relevant fields based on resource type."""
    resource_type = resource.get("resourceType", "N/A")
    common_details = {
        "ResourceType": resource_type,
        "Id": resource.get("id", "N/A")
    }

    if resource_type == "Patient":
        common_details.update({
            "Full Names": "; ".join(f"{name.get('prefix', [''])[0]} {name.get('given', [''])[0]} {name.get('family', '')}" for name in resource.get("name", [])),
            "Gender": resource.get("gender", "N/A"),
            "Birthdate": resource.get("birthDate", "N/A"),
            "Addresses": "; ".join(f"{addr.get('line', [''])[0]}, {addr.get('city', '')}, {addr.get('state', '')}, {addr.get('postalCode', '')}, {addr.get('country', '')}" for addr in resource.get("address", [])),
            "Contact Numbers": "; ".join(tel.get("value", "N/A") for tel in resource.get("telecom", [])),
            "Marital Status": resource.get("maritalStatus", {}).get("text", "N/A")
        })
    elif resource_type == "Encounter":
        common_details.update({
            "Status": resource.get("status", "N/A"),
            "Class": resource.get("class", {}).get("code", "N/A"),
            "Type": "; ".join(t.get("text", "N/A") for t in resource.get("type", [])),
            "Period": f"{resource.get('period', {}).get('start', 'N/A')} to {resource.get('period', {}).get('end', 'N/A')}"
        })

    return common_details

def format_resource_details(details):
    """Formats the resource details into a multi-line string for display."""
    return "\n".join(f"{key}: {value}" for key, value in details.items())

# Load JSON data
try:
    with open('bundle.json', 'r') as file:
        json_data = json.load(file)
except Exception as e:
    print(f"Error loading JSON file: {e}")
    exit(1)

# Process resources dynamically
resource_types = 'Patient',  # Extend this list as needed
for r_type in resource_types:
    resources = extract_from_bundle(json_data, r_type)
    for resource in resources:
        resource_details = parse_resource(resource)
        print(format_resource_details(resource_details))
        print("\n---\n")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from typing import Any, Type, Callable, Dict

class Converter:
    """
    A class for converting values between different types.
    """
    
    DIRECT_MAPPINGS = {"int", "float", "str", "bool"} # str(5.4), float(True)
    LOGICAL_MAPPINGS = {
        ("str", "list"): lambda x, *args, **kwargs: x.split(*args, **kwargs), #list("word") -> ['w','o','r', 'd'], instead we want: list("my name") -> ["my", "name"]
        ("list", "str"): lambda x, *args, **kwargs: " ".join(str(item) for item in x),
    }

    def __new__(cls, value: Any, target_type: Type, *args, **kwargs) -> Any:
        """
        Converts the value to the specified target type.

        Parameters:
            value (Any): The value to be converted.
            target_type (Type): The target type to convert the value to.
 
        Returns:
            Any: The converted value in the target type.
        """
        
        source_type_name = type(value).__name__
        target_type_name = target_type.__name__

        if source_type_name == target_type_name:
            return value
        elif {source_type_name, target_type_name}.issubset(cls.DIRECT_MAPPINGS):
            return target_type(value)
        else:
            conversion_key = (source_type_name, target_type_name)
            if conversion_key in cls.LOGICAL_MAPPINGS:
                return cls.LOGICAL_MAPPINGS[conversion_key](value, *args, **kwargs)
            else:
                raise ValueError(f"Conversion from {source_type_name} to {target_type_name} is not supported.")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC df row -> classA obj -> General Decoder -> classB obj -> df row

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

data_a = [("Alice", 30), ("Bob", 25)]
df_a = spark.createDataFrame(data_a, ["name", "age"])

data_b = [(["Ana", "Maria"], "30"), (["Tom", "Second"], "25")]
df_b = spark.createDataFrame(data_a, ["givenName", "myAge"])

# COMMAND ----------

#Let's assume that ClassA and ClassB are generated from a given spark schema
class ClassA:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f"ClassA(name={self.name}, age={self.age})"


class ClassB:
    def __init__(self, givenName, myAge):
        self.givenName = givenName
        self.myAge = myAge

    def __repr__(self):
        return f"ClassA(givenName={self.givenName}, myAge={self.myAge})"


# COMMAND ----------

# Create an object of ClassA from spark row
def row_to_object(spark_row, ClassA):
    return ClassA(**spark_row.asDict())

# COMMAND ----------

for row in df_a.collect():
    obj = row_to_object(row, ClassA)
    print(obj)

# COMMAND ----------

#use General cutsom decoder to convert object of ClassA to object of ClassB
source_person = ClassA("Alice", 30)
custom_person = ClassB(["Ana", "Maria"], "30")

source_attrs = source_person.__dict__
target_attrs = custom_person.__dict__

decoded_object = []
for source_value, target_value in zip(source_attrs.values(), target_attrs.values()):
    decoded_object.append(Converter(source_value, type(target_value)))

decoded_person = ClassB(*decoded_object)
print(decoded_person)

# COMMAND ----------

# convert object of classB to a spark row
from pyspark.sql import Row

def object_to_row(object):
    return Row(**object.__dict__)

# COMMAND ----------

print(object_to_row(decoded_person))

# COMMAND ----------

# MAGIC %md
# MAGIC ## new Version based on you e-mail
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
import json

# Initialize Spark session
spark = SparkSession.builder.appName("FHIRDataTransformation").getOrCreate()

# Define the schema for the final DataFrame
schema = StructType([
    StructField("patientId", StringType(), True),
    StructField("fullName", StringType(), True),
    StructField("birthDate", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("relationshipStatus", StringType(), True),
    StructField("primaryLanguage", StringType(), True),
    StructField("contactPhone", StringType(), True),
    StructField("driverLicense", StringType(), True),
    StructField("passportNumber", StringType(), True),
    StructField("medicalRecordNumber", StringType(), True),
    StructField("socialSecurityNumber", StringType(), True),
    StructField("mothersMaidenName", StringType(), True),
    StructField("race", StringType(), True),
    StructField("ethnicity", StringType(), True)
])

# Define ClassA and ClassB
class ClassA:
    def __init__(self, patient):
        self.patient = patient

class ClassB:
    def __init__(self, patientId, fullName, birthDate, gender, address, relationshipStatus,
                 primaryLanguage, contactPhone, driverLicense, passportNumber, medicalRecordNumber,
                 socialSecurityNumber, mothersMaidenName, race, ethnicity):
        self.patientId = patientId
        self.fullName = fullName
        self.birthDate = birthDate
        self.gender = gender
        self.address = address
        self.relationshipStatus = relationshipStatus
        self.primaryLanguage = primaryLanguage
        self.contactPhone = contactPhone
        self.driverLicense = driverLicense
        self.passportNumber = passportNumber
        self.medicalRecordNumber = medicalRecordNumber
        self.socialSecurityNumber = socialSecurityNumber
        self.mothersMaidenName = mothersMaidenName
        self.race = race
        self.ethnicity = ethnicity

def get_identifier_value(identifiers, code_type):
    for identifier in identifiers:
        if 'type' in identifier and 'coding' in identifier['type']:
            if any(coding['code'] == code_type for coding in identifier['type']['coding']):
                return identifier['value']
    return None

def get_extension_value(extensions, url, key='valueString'):
    for extension in extensions:
        if extension['url'] == url:
            if 'extension' in extension:
                for sub_extension in extension['extension']:
                    if key in sub_extension:
                        return sub_extension[key]
            if key in extension:
                return extension[key]
    return None

def transformation_logic():
    def transform(a_instance):
        patient = a_instance.patient
        identifiers = patient.get('identifier', [])
        extensions = patient.get('extension', [])
        
        patientId = patient['id']
        fullName = " ".join(patient['name'][0]['given']) + " " + patient['name'][0]['family']
        birthDate = patient.get('birthDate', '')
        gender = patient.get('gender', '')
        address = " ".join(patient['address'][0].get('line', [])) + ", " + ", ".join(
            [patient['address'][0].get(key, '') for key in ['city', 'state', 'postalCode', 'country']]
        )
        relationshipStatus = patient['maritalStatus'].get('text', '')
        primaryLanguage = patient['communication'][0]['language'].get('text', '') if 'communication' in patient and len(patient['communication']) > 0 else ''
        contactPhone = next((x['value'] for x in patient['telecom'] if x['system'] == 'phone'), None)
        driverLicense = get_identifier_value(identifiers, 'DL')
        passportNumber = get_identifier_value(identifiers, 'PPN')
        medicalRecordNumber = get_identifier_value(identifiers, 'MR')
        socialSecurityNumber = get_identifier_value(identifiers, 'SS')
        mothersMaidenName = get_extension_value(extensions, "http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName")
        race = get_extension_value(extensions, "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race", 'valueString')
        ethnicity = get_extension_value(extensions, "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity", 'valueString')
        
        return ClassB(patientId, fullName, birthDate, gender, address, relationshipStatus, primaryLanguage, contactPhone, driverLicense, passportNumber, medicalRecordNumber, socialSecurityNumber, mothersMaidenName, race, ethnicity)
    return transform

# Read the JSON file
with open("path/to/you/jsonfile.json", 'r') as file:
    data = json.load(file)

# Extract the patient resource
patient_data = [entry['resource'] for entry in data['entry'] if entry['resource']['resourceType'] == 'Patient'][0]

# Create an RDD of ClassA instances
rdd_a = spark.sparkContext.parallelize([ClassA(patient_data)])

# Apply the transformation
transform_func = transformation_logic()
rdd_b = rdd_a.map(transform_func)

# Convert ClassB instances to Spark Rows
def class_b_to_row(b_instance):
    return Row(
        patientId=b_instance.patientId,
        fullName=b_instance.fullName,
        birthDate=b_instance.birthDate,
        gender=b_instance.gender,
        address=b_instance.address,
        relationshipStatus=b_instance.relationshipStatus,
        primaryLanguage=b_instance.primaryLanguage,
        contactPhone=b_instance.contactPhone,
        driverLicense=b_instance.driverLicense,
        passportNumber=b_instance.passportNumber,
        medicalRecordNumber=b_instance.medicalRecordNumber,
        socialSecurityNumber=b_instance.socialSecurityNumber,
        mothersMaidenName=b_instance.mothersMaidenName,
        race=b_instance.race,
        ethnicity=b_instance.ethnicity
    )

rows_rdd = rdd_b.map(class_b_to_row)
df = spark.createDataFrame(rows_rdd, schema)
df.show()


# COMMAND ----------

display(df)

# COMMAND ----------

df.select("fullName", "birthDate", "gender").show()

# COMMAND ----------

df.collect()

# COMMAND ----------


