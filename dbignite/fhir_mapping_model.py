import os, sys, json, re
from pyspark.sql.types import *
from importlib.resources import files
# from dbignite.fhir_dict_object import *

class fhirSchemaModel():
    def __init__(self, fhir_resource_map = None):
        
        # Quicker runtime with package_data so there is no reliance on directory structure 
        self.fhir_resource_map = { str(x).rsplit('/',1)[1][:-5] : StructType.fromJson(json.load(open(x, "r"))) for x in list(files("schemas").iterdir())}
        # Quickest runtime from a single python file with struct information (0.1 seconds)
        # self.python_struct_fhir_resource_map = fhir_dict_map

        # Lookup of files from schema directory 
        # self.fhir_resource_map = {x[:-5]: StructType.fromJson(json.load(open("../schemas/" + x, "r"))) for x in os.listdir("../schemas")}


    def resource(self, resourceName: str) -> str:
      return self.fhir_resource_map[resourceName]
    
    # Adding due to ambiguity on resource / schema retrieval call 
    def schema(self, resourceName: str) -> str:
      return self.fhir_resource_map[resourceName]

    # Debugging keys  
    def debug_print_keys(self):
      print(self.fhir_resource_map.keys())

    def list_packaged_data(self):
      for x in list(files("schemas").iterdir()):
        print(x)
