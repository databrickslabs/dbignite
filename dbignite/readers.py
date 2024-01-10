from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from enum import Enum
from dbignite.fhir_resource import FhirResource

class FhirFormat(Enum):
    BUNDLE = 1
    NDJSON = 2
    BULK = 3

#
# default of FHIR bundle reads
#
def read_from_directory(path: str,
                        resource_format = FhirFormat.BUNDLE,
                        spark: SparkSession = SparkSession.getActiveSession()) -> FhirResource:

    data = spark.read.text(path, wholetext=True).select(col("value").alias("resource"))
    if resource_format == FhirFormat.BUNDLE:
        return FhirResource.from_raw_bundle_resource(data)
    elif resource_format == FhirFormat.NDJSON:
        return FhirResource.from_raw_ndjson_resource(data)
    elif resource_format == FhirFormat.BULK:
        raise("Bulk format not implemented")
    else:
        raise("Unrecongized FhirFormat: " + str(resource_format))


def read_from_stream() -> FhirResource:
    pass #raise NotImplementedError
