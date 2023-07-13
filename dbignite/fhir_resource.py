from abc import ABC, abstractmethod
from typing import ClassVar, Optional, cast
from dbignite.fhir_mapping_model import FhirSchemaModel

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
    sum
)
from pyspark.sql.types import ArrayType, StringType, StructType


class FhirResource(ABC):
    @abstractmethod
    def __init__(self, raw_data: DataFrame) -> None:
        ...

    @abstractmethod
    def print_summary(self) -> None:
        ...

    @abstractmethod
    def read_data(self) -> DataFrame:
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


class GenericFhirResource(FhirResource):
    def __init__(self, raw_data: DataFrame) -> None:
        self.__raw_data = raw_data

    def print_summary(self) -> None:
        print(f"Total resources: {self.__raw_data.count()}")

    @staticmethod
    def resource_type() -> str:
        return "Generic"

    def read_data(self) -> DataFrame:
        return super().read_data(resource_schemas)


class BundleFhirResource(FhirResource):
    BUNDLE_SCHEMA: ClassVar[StructType] = (
        StructType()
        .add("resourceType", StringType())
        .add("entry", ArrayType(StructType().add("resource", StringType())))
    )

    def __init__(self, raw_data: DataFrame, fhir_resource_schema = FhirSchemaModel()) -> None:
        self.__raw_data = raw_data
        self.resource_schemas = fhir_resource_schema #optional, may be good to cache large object
        self.entry = None

    def print_summary(self) -> None:
        print(f"Total bundles: {self.__raw_data.count()}")

    def read_entry(self) -> None:
        if self.entry is None:
            self.entry= self.read_data()

    #
    # Count across all bundles
    #
    def count_resource_type(self, resource_type_name, column_alias="resource_sum"):
        return self.entry.select(sum(size(col(resource_type_name))).alias(column_alias))

    #
    # Count within bundle
    #
    def count_within_bundle_resource_type(self, resource_type_name, column_alias="resource_bundle_sum"):
        return self.entry.select(size(col(resource_type_name)).alias(column_alias))
        
    def read_data(self) -> DataFrame:
        bundle = from_json("resource", BundleFhirResource.BUNDLE_SCHEMA).alias("bundle")
        resource_columns = [
            self.__convert_from_json(
                self.__filter_resources(col("bundle.entry.resource"), resource_type),
                schema,
                resource_type,
            )
            for resource_type, schema in self.resource_schemas.fhir_resource_map.items()
            if resource_type.upper() != "BUNDLE"
        ]

        return self.__raw_data.select(bundle).select(resource_columns)

    @staticmethod
    def resource_type() -> str:
        return "Bundle"

    #
    # Given a FHIR resource name, return all matching entries
    #
    def __filter_resources(self, column: Column, resource_type: str) -> Column:
        return filter(
            column,
            lambda x: upper(get_json_object(x, "$.resourceType"))
            == lit(resource_type.upper()),
        )

    #
    # Given a schema, return a column mapped to the schema
    #
    def __convert_from_json(
        self, column: Column, schema: StructType, resource_type: str
    ) -> Column:
        if column is None:
            return lit(None)
        return transform(column, lambda x: from_json(x, schema)).alias(resource_type)


class MultipleResourceTypesException(Exception):
    pass


class NoResourcesException(Exception):
    pass


class InvalidSchemaException(Exception):
    pass
