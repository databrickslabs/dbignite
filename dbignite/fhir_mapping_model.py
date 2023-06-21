import os, sys, json
from pyspark.sql.types import *

class fhirSchemaModel():
    def __init__(self, mapping = None):
        self.mapping = self.createFHIRMapping()

    def createFHIRMapping(self) -> dict[str, str]:
      schema_path = "../schemas"
      schema_dir = os.listdir( schema_path )

      resource_map = {}

      for file in schema_dir:
        with open("../schemas/" + file) as schema_file:
          schema_struct = StructType.fromJson(json.load(schema_file))
          resource_map[file.replace('.json', '')] = schema_struct
        break
      return resource_map
    
    def resource(self, resourceName: str) -> str:
      return self.mapping[resourceName]
    
    # Adding due to ambiguity on resource / schema retrieval call 
    def schema(self, resourceName: str) -> str:
      return self.mapping[resourceName]

    #debugging    
    def debug_print_keys(self):
      print(self.mapping.keys())