from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from .fhir_resource import FhirResource
def read_from_directory(
    path: str, spark: SparkSession = SparkSession.getActiveSession()
) -> FhirResource:
    raw_data = spark.read.text(path, wholetext=True).select(
        col("value").alias("resource")
    )
    return FhirResource.from_raw_resource(raw_data)


def read_from_stream() -> FhirResource:
    # TODO
    raise NotImplementedError
