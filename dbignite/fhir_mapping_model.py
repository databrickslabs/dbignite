import os, sys, json
from pyspark.sql.types import *
from fhir_dict_object import fhir_dict_map

class fhirSchemaModel():
    def __init__(self, mapping = None):
        self.mapping = None #.fhir_dict_map
    
    def resource(self, resourceName: str) -> str:
      return self.mapping[resourceName]
    
    # Adding due to ambiguity on resource / schema retrieval call 
    def schema(self, resourceName: str) -> str:
      return self.mapping[resourceName]

    # Debugging    
    def debug_print_keys(self):
      print(self.mapping.keys())