import os, sys, json, re
from pyspark.sql.types import *
from importlib.resources import files


class FhirSchemaModel:
    #
    # Class that manages access to FHIR resourceType ->  Spark Schema mapping
    #
    def __init__(self, fhir_resource_map=None):
        # Create mapping with ALL FHIR Resources, key,resourceName -> value,sparkSchema
        self.fhir_resource_map = (
            {
                str(x)[:-5]: StructType.fromJson(json.load(open(str(files("dbignite")) + '/schemas/' + x, "r")))
                for x in os.listdir(str(files("dbignite")) + '/schemas')
             }
            if fhir_resource_map is None
            else fhir_resource_map
        )

    #
    # Given a resourceName, return the spark schema representation
    #
    def schema(self, resourceName):
        return self.fhir_resource_map[resourceName]

    #
    # Load all FHIR resources into one dictionary
    #

    @classmethod
    def all_fhir_resource_mapping(cls):
        return {
            str(x)[:-5]: StructType.fromJson(json.load(open(x, "r")))
            for x in list(files("schemas").iterdir())
        }

    #
    # Load US Core FHIR resources into one dictionary
    #

    @classmethod
    def us_core_fhir_resource_mapping(cls):
        us_core_resources = [
            "AllergyIntolerance",
            "CarePlan",
            "CareTeam",
            "Condition",
            "Coverage",
            "Device",
            "DiagnosticReport",
            "DocumentReference",
            "Encounter",
            "Goal",
            "Immunization",
            "Location",
            "Medication",
            "MedicationDispense",
            "MedicationRequest",
            "Observation",
            "Organization",
            "Patient",
            "Practitioner",
            "Procedure",
            "Provenance",
            "Questionnaire",
            "QuestionnaireResponse",
            "RelatedPerson",
            "ServiceRequest",
            "Specimen",
        ]
        us_core_mapping = {
            x: StructType.fromJson(json.load(open(str(files("schemas")) + '/schemas/' + x + '.json', "r")))
            for x in us_core_resources
        }
        return FhirSchemaModel(fhir_resource_map=us_core_mapping)

    #
    # Load supplied subset of FHIR resources into one dictionary
    #

    @classmethod
    def custom_fhir_resource_mapping(cls, resource_list):
        custom_mapping = {
            x: StructType.fromJson(
                json.load(open(files("schemas").joinpath(x + ".json"), "r"))
            )
            for x in resource_list
        }
        return FhirSchemaModel(fhir_resource_map=custom_mapping)

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
        pass  # TODO method to search JSON metadata

    #
    # Allow searching for fields contained in the spark schema
    #
    def search_columns(self, search_expression, within_resource=None):
        pass  # TODO method to search column names
