from abc import ABC, abstractmethod
from multiprocessing.pool import ThreadPool
import multiprocessing as mp
from typing import ClassVar, Optional, cast
from dbignite.fhir_mapping_model import FhirSchemaModel
from .fhir_mapping_model import FhirSchemaModel
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    filter,
    from_json,
    get_json_object,
    lit,
    transform,
    upper,
    size,
    sum,
)
from pyspark.sql.types import ArrayType, StringType, StructType


class FhirResource(ABC):
    @abstractmethod
    def __init__(self, raw_data: DataFrame) -> None:
        ...

    @abstractmethod
    def read_data(self, schemas: Optional[FhirSchemaModel] = None) -> DataFrame:
        ...

    @staticmethod
    @abstractmethod
    def resource_type() -> str:
        ...

    @staticmethod
    def from_raw_resource(
        data: DataFrame, resource_type: Optional[str] = None
    ) -> "FhirResource":
        if not any(
            c
            for c in data.schema
            if c.name.lower() == "resource" and c.dataType.typeName() == "string"
        ):
            data.printSchema()
            raise InvalidSchemaException("No resource column of type string found.")

        if not resource_type:
            resource_types = (
                data.select(
                    get_json_object("resource", "$.resourceType").alias("resourceType")
                )
                .dropna()
                .distinct()
                .take(2)
            )
            if len(resource_types) > 1:
                raise MultipleResourceTypesException(
                    "Found more than 1 resource type in data."
                )

            if len(resource_types) == 0:
                raise NoResourcesException("No resources found.")

            resource_type = cast(str, resource_types[0][0])

        resource_classes = [
            resource
            for resource in FhirResource.__subclasses__()
            if resource.resource_type().upper() == resource_type.upper()
        ]
        resource: FhirResource
        if len(resource_classes) == 0:
            resource = GenericFhirResource(data)
        else:
            resource = resource_classes[0](data)  # type: ignore

        return resource

#
# Core representation of FHIR bundles
#  BUNDLE_SCHEMA - lightweight representation of first level json schema
#  raw_data - raw json of FHIR bundle
#  entry - internal representation of entry schema for FHIR
#
class BundleFhirResource(FhirResource):
    BUNDLE_SCHEMA: ClassVar[StructType] = (
        StructType()
        .add("resourceType", StringType())
        .add("entry", ArrayType(StructType().add("resource", StringType())))
        .add("id", StringType())
        .add("timestamp", StringType())
    )

    #
    #
    #
    def __init__(self, raw_data: DataFrame) -> None:
        self.__raw_data = raw_data
        self.__entry: Optional[DataFrame] = None


    @property
    def entry(self) -> DataFrame:
        if self.__entry is None:
            self.__entry = self.read_data()
        return self.__entry

    #
    # Count across all bundles
    #
    def count_resource_type(
        self, resource_type: str, column_alias: str = "resource_sum"
    ) -> DataFrame:
        return self.entry.select(sum(size(col(resource_type))).alias(column_alias))

    #
    # Count within bundle
    #
    def count_within_bundle_resource_type(
        self, resource_type: str, column_alias: str = "resource_bundle_sum"
    ) -> DataFrame:
        return self.entry.select(size(col(resource_type)).alias(column_alias))

    #
    # Read and parse all data in raw_data 
    #
    def read_data(self, schemas: Optional[FhirSchemaModel] = None) -> DataFrame:
        if not schemas:
            schemas = FhirSchemaModel()

        bundle = from_json("resource", BundleFhirResource.BUNDLE_SCHEMA).alias("bundle")
        resource_columns = [
            self.__convert_from_json(
                self.__filter_resources(col("bundle.entry.resource"), resource_type),
                schema,
                resource_type,
            )
            for resource_type, schema in schemas.fhir_resource_map.items()
            if resource_type.upper() != "BUNDLE"
        ] + [col("bundle.timestamp"), col("bundle.id")]
        #Root level columns, timestamp & id to include in the resource

        return self.__raw_data.select(bundle).select(resource_columns)

    #
    # Given a schema, return a column mapped to the schema
    #  @param column - the column to parse json data from
    #  @param schema - the schema of the column to build
    #  @param resource_type - the FHIR resource being parsed (must be filtered prior to this step)
    #  @return a new column named resource_type with specified spark_schema
    #
    def __convert_from_json(
        self, column: Column, schema: StructType, resource_type: str
    ) -> Column:
        if column is None:
            return lit(None)
        return transform(column, lambda x: from_json(x, schema)).alias(resource_type)

    #
    # Static "Bundle" resource 
    #
    @staticmethod
    def resource_type() -> str:
        return "Bundle"

    #
    # Given a FHIR resource name, return all matching entries
    #  @param column - of the self.entry DF
    #  @param resource_type - FHIR resource name to parse
    #  @return all columns that match the resource type 
    #
    def __filter_resources(self, column: Column, resource_type: str) -> Column:
        return filter(
            column,
            lambda x: upper(get_json_object(x, "$.resourceType"))
            == lit(resource_type.upper()),
        )


    #
    # Bulk write resources as relational tables, 1 table per FHIR resource
    #  @param columns - list of columns to write, default None = write all in Dataframe
    #  @param location - catalog.database.schema definition of where to write
    #  @param write_mode - spark's write mode, default of append to existing tables
    #  @return None
    #
    def bulk_table_write(self, location = "",  write_mode = "append", columns = None):
        pool = ThreadPool(mp.cpu_count()-1)
        list(pool.map(lambda x: write_table(x, location, write_mode), (self.__entry if columns is None else columns)))

    #
    # Write an individual FHIR resource as a table
    #
    def write_table(self, column, location = "", write_mode = "append"):
        self.__entry.select(col("timestamp"),col("id"),column).write.mode(write_mode).saveAsTable( (location + "." + column).lstrip("."))
