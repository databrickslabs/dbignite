import json

from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterable, Type

from pyspark.sql import DataFrame
from pyspark.sql.catalog import Database


from dbignite.transformers import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = (SparkSession.builder.appName("myapp") \
                      .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
                      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                      .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                      .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
                      .master("local") \
                      .getOrCreate())
spark.conf.set("spark.sql.shuffle.partitions", 1)


class DataModel(ABC):
  
  @abstractmethod
  def summary(self) -> DataFrame:
    ...
    
  @abstractmethod
  def listDatabases(self) -> Iterable[Database]:
    ...
    

class FhirBundles(DataModel):
  
  def __init__(self, path: str):
    self.path = path
    
  def listDatabases():
    raise NotImplementedError()

  def summary():
    raise NotImplementedError() 


class PersonDashboard(DataModel):
  
  @classmethod
  def builder(cls, from_: DataModel, cdm_database : str, cdm_mapping_database: str, append: bool):
    if isinstance(from_, FhirBundles):
      return cls._from_fhir_bundles(from_,cdm_database,cdm_mapping_database, append)
    else:
      raise NotImplementedError()
  
  def __init__(self, df: DataFrame):
    self.df = df
    
  def summary(self):
    return self.df
  
  def listDatabases(self):
    raise NotImplementedError()
    
  @staticmethod
  def _from_fhir_bundles(from_: FhirBundles, cdm_database : str, cdm_mapping_database: str, append: bool):
    omop_cdm = fhir_bundles_to_omop_cdm(from_.path, cdm_database, cdm_mapping_database, append)
    person_dashboard = omop_cdm_to_person_dashboard(*omop_cdm.listDatabases())
    return person_dashboard


class OmopCdm(DataModel):
  
  def __init__(self, cdm_database: str, mapping_database: str):
    self.cdm_database = cdm_database
    self.mapping_database = mapping_database
    
  def summary(self) -> DataFrame:
    raise NotImplementedError()
  
  def listDatabases(self):
    return (self.cdm_database, self.mapping_database)

## transformers

def fhir_bundles_to_omop_cdm(path: str, cdm_database: str, mapping_database: str, overwrite: bool) -> OmopCdm:
  entries_df = (
    spark.read.text(path, wholetext=True)
    .select(explode(_entry_json_strings('value')).alias('entry_json'))
    .withColumn('entry', from_json('entry_json', schema=ENTRY_SCHEMA))
  ).cache()

  person_df = entries_to_person(entries_df)
  condition_df = entries_to_condition(entries_df)
  procedure_occurrence_df = entries_to_procedure_occurrence(entries_df)
  encounter_df = entries_to_encounter(entries_df)

  spark.sql(f'CREATE DATABASE IF NOT EXISTS {cdm_database}')
  spark.sql(f'CREATE DATABASE IF NOT EXISTS {mapping_database}')
  spark.catalog.setCurrentDatabase(cdm_database)
  
  if overwrite:
    person_df.write.format("delta").mode("overwrite").saveAsTable(PERSON_TABLE)
    condition_df.write.format("delta").mode("overwrite").saveAsTable(CONDITION_TABLE)
    procedure_occurrence_df.write.format("delta").mode("overwrite").saveAsTable(PROCEDURE_OCCURRENCE_TABLE)
    encounter_df.write.format("delta").mode("overwrite").saveAsTable(ENCOUNTER_TABLE)

  else:
    person_df.write.format("delta").saveAsTable(PERSON_TABLE)
    condition_df.write.format("delta").saveAsTable(CONDITION_TABLE)
    procedure_occurrence_df.write.format("delta").saveAsTable(PROCEDURE_OCCURRENCE_TABLE)
    encounter_df.write.format("delta").saveAsTable(ENCOUNTER_TABLE)

  return OmopCdm(cdm_database, mapping_database)


@udf(ArrayType(StringType()))
def _entry_json_strings(value):
  '''
  UDF takes raw text, returns the
  parsed struct and raw JSON.
  '''
  bundle_json = json.loads(value)
  return [json.dumps(e) for e in bundle_json['entry']]


def omop_cdm_to_person_dashboard(cdm_database: str, mapping_database: str) -> PersonDashboard:
  spark.sql(f'USE {cdm_database}')
  person_df = spark.read.table(PERSON_TABLE)
  condition_df = spark.read.table(CONDITION_TABLE)
  procedure_occurrence_df = spark.read.table(PROCEDURE_OCCURRENCE_TABLE)
  
  encounter_df = spark.read.table(ENCOUNTER_TABLE)
  
  condition_summary_df = summarize_condition(condition_df)
  procedure_occurrence_summary_df = summarize_procedure_occurrence(procedure_occurrence_df)
                                  
  encounter_summary_df = summarize_encounter(encounter_df)
  
  return PersonDashboard(
    person_df
    .join(condition_summary_df, 'person_id', 'left')
    .join(procedure_occurrence_summary_df, 'person_id', 'left')
    .join(encounter_summary_df, 'person_id', 'left')
  )
