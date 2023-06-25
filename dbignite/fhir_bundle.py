from abc import ABC, abstractmethod
from typing import Optional, cast

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode


class FhirBundles():
    """
     Internally represented as a DataFrame of:
       raw_data: String of original FHIR Bundle
       entries: a flattening of all entries in a dataframe
       entries.XXX : Where each resourceType of FHIR is XXX and contains the values 
       is_valid: True if the bundle was a valid entry or not (non-valid is not parsed into entries)
    Note: 1 row per FHIRBundle
    """
    def __init__(self):
        ...
        
    def num_bundles(self):
        return self.df.count()

    def num_entries(self):
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
