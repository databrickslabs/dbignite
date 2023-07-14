import os, json
from typing import Optional, ClassVar
from importlib.resources import files
from pyspark.sql.types import StructType


class FhirSchemaModel:

    #
    # Class that manages access to FHIR resourceType ->  Spark Schema mapping
    #
    def __init__(
        self, fhir_resource_map: Optional[dict[str, StructType]] = None
    ) -> None:
        self.__fhir_resource_map = (
            {
                resource_type: FhirSchemaModel.__read_schema(schema_path)
                for resource_type, schema_path in FhirSchemaModel.__get_schema_paths()
            }
            if fhir_resource_map is None
            else fhir_resource_map
        )


    @classmethod
    def __read_schema(cls, path: str) -> StructType:
        with open(path, "r") as f:
            return StructType.fromJson(json.load(f))


    @classmethod
    def __get_schema_paths(cls) -> list[tuple[str, str]]:
        schema_dir = str(files("dbignite")) + "/schemas"
        return [
            (os.path.splitext(p)[0], os.path.join(schema_dir, p))
            for p in os.listdir(schema_dir)
            if p.endswith(".json")
        ]

    #
    # Given a resourceName, return the spark schema representation
    #
    def schema(self, resource_type: str) -> StructType:
        return self.__fhir_resource_map[resource_type]
    

    #
    # Returns a dictionary of all the schemas that are included in this FhirMappingModel
    #
    @property
    def fhir_resource_map(self) -> dict[str, StructType]:
        return self.__fhir_resource_map

    #
    # Load all FHIR resources into one dictionary
    #
    def all_fhir_resource_mapping(cls) -> "FhirSchemaModel":
        return FhirSchemaModel()

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
        return FhirSchemaModel.custom_fhir_resource_mapping(us_core_resources)

    #
    # Load supplied subset of FHIR resources into one dictionary
    #
    @classmethod
    def custom_fhir_resource_mapping(cls, resource_list: list[str]) -> "FhirSchemaModel":
        custom_mapping = {
            resource_type: FhirSchemaModel.__read_schema(schema_path)
            for resource_type, schema_path in FhirSchemaModel.__get_schema_paths()
            if resource_type in resource_list
        }
        return FhirSchemaModel(fhir_resource_map=custom_mapping)

    #
    # Return all keys of FHIR Resource References
    #
    def list_keys(self):
        return list(self.__fhir_resource_map.keys())

    #
    # Return all keys of FHIR Resources packaged
    #
    def list_packaged_data(self):
        return [resource_type for resource_type, _ in FhirSchemaModel.__get_schema_paths()]

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
