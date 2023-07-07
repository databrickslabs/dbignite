import os, sys, json, re
from pyspark.sql.types import *
from importlib.resources import files

class FhirSchemaModel():

    #
    # Class that manages access to FHIR resourceType ->  Spark Schema mapping
    #
    def __init__(self, fhir_resource_map = None, subset=None):

        # Create mapping with ALL Fhir Resources 
        if fhir_resource_map is None and subset == None:
            self.fhir_resource_map = { str(x).rsplit('/',1)[1][:-5] : StructType.fromJson(json.load(open(x, "r"))) for x in list(files("schemas").iterdir())}
        
        # Create mapping with FHIR US Core 
        if fhir_resource_map is None and subset == "UScore":
            us_core_resources = ["AllergyIntolerance", "Bundle", "CarePlan", "CareTeam", "Condition", "Coverage", "Device", "DiagnosticReport", "DocumentReference", "Encounter", "Goal", "Immunization", "Location", "Medication", "MedicationDispense", "MedicationRequest", "Observation", "Organization", "Patient", "Practitioner", "Procedure", "Provenance", "Questionnaire", "QuestionnaireResponse", "RelatedPerson", "ServiceRequest", "Specimen
            self.fhir_resource_map = { x : StructType.fromJson(json.load(open(files('schemas').joinpath(x + ".json"), "r"))) for x in us_core_resources}
        
        # Set the mapping with provided parameter 
        else:
            self.fhir_resource_map = fhir_resource_map

    #
    # Given a resourceName, return the spark schema representation
    #
    def schema(self, resourceName):
        return self.fhir_resource_map[resourceName]

    #
    # Return all keys of FHIR Resource References
    #
    def list_keys(self):
        return list(self.fhir_resource_map.keys())
      
    #
    # Return all keys of FHIR Resources pacakged 
    #
    def list_packaged_data(self):
        return list(files("schemas").iterdir())

    #
    # Allow searching at the metadata level contained in the spark schema 
    #
    def search_metadata(self, search_expression, within_resource=None):
        pass #TODO method to search JSON metadata

    #
    # Allow searching for fields contained in the spark schema 
    #
    def search_columns(self, search_expression, within_resource=None):
        pass #TODO method to search column names
