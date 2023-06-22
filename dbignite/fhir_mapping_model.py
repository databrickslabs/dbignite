import os, sys, json
from pyspark.sql.types import *

class fhirSchemaModel():
    def __init__(self, mapping = None):
        self.mapping = self.createFHIRMapping()

    # Returns dict with [str, StructType]
    def createFHIRMapping(self):
      # TODO from pip package, reference { streamread / fileread from package }
      schema_dir = os.listdir( "../schemas" )
      resource_map = {}
      # no FOR loops anymore for William :(
      for file in schema_dir:
        with open("../schemas/" + file) as schema_file:
          schema_struct = StructType.fromJson(json.load(schema_file))
          resource_map[file.replace('.json', '')] = schema_struct
      return resource_map
    
    def resource(self, resourceName: str) -> str:
      return self.mapping[resourceName]
    
    # Adding due to ambiguity on resource / schema retrieval call 
    def schema(self, resourceName: str) -> str:
      return self.mapping[resourceName]

    # Debugging    
    def debug_print_keys(self):
      print(self.mapping.keys())