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

      return resource_map
    
    def getResourceSchema(self, resourceName: str):
      return self.mapping[resourceName]