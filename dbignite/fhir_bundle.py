from abc import ABC, abstractmethod
from typing import Optional, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode


class FhirBundles():
    """
     Internally represented as a DataFrame (self.df) of:
       raw_data: String of original FHIR Bundle
       pk: random id generated to maintain references
       entries: a flattening of all entries in a dataframe
       entries.XXX : Where each resourceType of FHIR is XXX and contains the values 
       is_valid: True if the bundle was a valid entry or not (non-valid is not parsed into entries)
    Note: 1 row per FHIRBundle

    __init__ is responsible for reading in data from various data sources.
      it's default beavhior is to read a single FhirBundle in a text file
    
    """
    def __init__(self, fhir_mapping, load_data_frame_func = as_whole_text_file, **args):
        from pyspark.sql import SparkSession
        self.spark = SparkSession.getActiveSession()
        self.fhir_mapping = fhir_mapping
        self.load_data_frame_funct = load_data_frame_func
        self.args = args
        self.df = None

    def load_bundles(self):
        if self.df = None:
            self.df = self.load_data_frame_func(**self.args)

    #
    # TODO parse into self.df...
    #
    def as_whole_text_file(self, path):
        return (
            self.spark
              .read.text(path, wholetext=True)
              .rdd.map(lambda x: parse_bundle_text(x(1)))
        )

    #
    # Generate random unique key for PK 
    #
    def uuid(self):
        ...

    #
    # Generate an md5 from a string value
    #
    def md5(self, s):
        ...

    #
    # Todo parse json and return dataframe like structure
    #
    def parse_bundle_text(self, str):
        ...
        
    def num_bundles(self):
        return self.df.count()

    def num_entries(self):
        ...

    def _parse_string_df(self):
        ...
    
    @abstractmethod
    def print_summary(self) -> None:
        ...

    @staticmethod
    @abstractmethod
    def resource_type() -> str:
        ...

    @property
    def data(self) -> DataFrame:
        return self.__data

class FhirBundle(FhirResource):
    def __init__(self, data: DataFrame) -> None:
        super().__init__(data)
        self.__resources = data.select(explode("entry.resource")).select("col.*")

    def print_summary(self) -> None:
        print(f"Total bundles: {self.data.count()}")
        print(f"Total resources: {self.__resources.count()}")
        display(
            self.__resources.groupBy("_metadata.file_path")
            .count()
            .orderBy("_metadata.file_path")
        )

        display(
            self.__resources.groupBy("resourceType").count().orderBy("resourceType")
        )

    @property
    def resources(self) -> dict[str, FhirResource]:
        return {
            r["resourceType"]: self.get_resource(r["resourceType"])
            for r in self.__resources.select("resourceType").distinct().collect()
        }

    def get_resource(self, resource_type: str) -> FhirResource:
        return build_fhir_resource(
            self.__resources.where(col("resourceType") == resource_type), resource_type
        )

    @staticmethod
    def resource_type() -> str:
        return "Bundle"
