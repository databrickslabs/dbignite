# Databricks notebook source
pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

!pip install dataclasses-json marshmallow

# COMMAND ----------

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from marshmallow import Schema, fields, post_load, INCLUDE
from dataclasses_json import dataclass_json
import json
# Define a data class for the Patient with optional fields
@dataclass_json
@dataclass
class Patient:
    id: str
    resourceType: str
    meta: Optional[Dict] = None
    text: Optional[Dict] = None
    extension: Optional[List[Dict]] = None
    identifier: Optional[List[Dict]] = None
    active: Optional[bool] = None  # Optional field
    name: Optional[List[Dict]] = None
    telecom: Optional[List[Dict]] = None
    gender: Optional[str] = None
    birthDate: Optional[str] = None
    address: Optional[List[Dict]] = None
    maritalStatus: Optional[Dict] = None
    communication: Optional[List[Dict]] = None
    deceasedBoolean: Optional[bool] = None
    multipleBirthBoolean: Optional[bool] = None
    photo: Optional[List[Dict]] = field(default_factory=list)
    contact: Optional[List[Dict]] = field(default_factory=list)
    generalPractitioner: Optional[List[Dict]] = field(default_factory=list)
    managingOrganization: Optional[Dict] = None
    link: Optional[List[Dict]] = field(default_factory=list)

# Define a marshmallow schema for the Patient with comprehensive fields
class PatientSchema(Schema):
    id = fields.Str(required=True)
    resourceType = fields.Str(required=True)
    meta = fields.Dict()
    text = fields.Dict()
    extension = fields.List(fields.Dict())
    identifier = fields.List(fields.Dict())
    active = fields.Bool()
    name = fields.List(fields.Dict())
    telecom = fields.List(fields.Dict())
    gender = fields.Str()
    birthDate = fields.Str()
    address = fields.List(fields.Dict())
    maritalStatus = fields.Dict()
    communication = fields.List(fields.Dict())
    deceasedBoolean = fields.Bool()
    multipleBirthBoolean = fields.Bool()
    photo = fields.List(fields.Dict())
    contact = fields.List(fields.Dict())
    generalPractitioner = fields.List(fields.Dict())
    managingOrganization = fields.Dict()
    link = fields.List(fields.Dict())

    class Meta:
        unknown = INCLUDE  # To handle any unexpected fields

    @post_load
    def make_patient(self, data, **kwargs):
        return Patient(**data)

# Function to load data from JSON file and extract patient information
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        for entry in data['entry']:
            if entry['resource']['resourceType'] == 'Patient':
                return entry['resource']
        return None

# Load patient data from a JSON file
patient_data = load_json_data('/Workspace/Users/islam.hoti@xponentl.ai/dbignite-forked/sampledata/Abe_Bernhard_4a0bf980-a2c9-36d6-da55-14d7aa5a85d9.json')

# Convert to Patient object using dataclasses-json
if patient_data:
    patient_obj = Patient.from_dict(patient_data)
    print("Converted using dataclasses-json:")
    print(patient_obj)

    # Convert using marshmallow
    patient_schema = PatientSchema()
    patient_obj = patient_schema.load(patient_data)
    print("\nConverted using marshmallow:")
    print(patient_obj)
else:
    print("No patient data found in the file.")


# COMMAND ----------


