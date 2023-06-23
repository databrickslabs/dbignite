import os, sys, json
from pyspark.sql.types import *
# from dbignite.fhir_dict_object import *

class fhirSchemaModel():
    def __init__(self, fhir_resource_map = None):
        self.fhir_resource_map = {x[:-5]: json.load(open("../schemas/" + x, "r")) for x in os.listdir("../schemas")}
    
    def resource(self, resourceName: str) -> str:
      return self.fhir_resource_map[resourceName]
    
    # Adding due to ambiguity on resource / schema retrieval call 
    def schema(self, resourceName: str) -> str:
      return self.fhir_resource_map[resourceName]

    # Debugging    
    def debug_print_keys(self):
      print(self.fhir_resource_map.keys())
