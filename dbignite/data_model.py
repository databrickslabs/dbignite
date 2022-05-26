import json

from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterable, Type

from pyspark.sql import DataFrame
from pyspark.sql.catalog import Database


from dbignite.utils import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


PERSON_TABLE = 'person'
CONDITION_TABLE = 'condition'
PROCEDURE_OCCURRENCE_TABLE = 'procedure_occurrence'
ENCOUNTER_TABLE = 'encounter'

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

  def __init__(self, df: DataFrame):
    self.df = df
    
  def summary(self):
    return self.df
  
  def listDatabases(self):
    raise NotImplementedError()


class OmopCdm(DataModel):
  
  def __init__(self, cdm_database: str, mapping_database: str):
    self.cdm_database = cdm_database
    self.mapping_database = mapping_database
    
  def summary(self) -> DataFrame:
    raise NotImplementedError()
  
  def listDatabases(self):
    return (self.cdm_database, self.mapping_database)

class Transformer():

  def __init__(self, spark):
    self.spark = spark 

  def load_entries_df(self, path):
    entries_df = (
      self.spark.read.text(path, wholetext=True)
      .select(explode(self._entry_json_strings('value')).alias('entry_json'))
      .withColumn('entry', from_json('entry_json', schema=ENTRY_SCHEMA))
    ).cache()
    return(entries_df)

  @staticmethod
  @udf(ArrayType(StringType())) ## TODO change to pandas_udf
  def _entry_json_strings(value):
    '''
    UDF takes raw text, returns the
    parsed struct and raw JSON.
    '''
    bundle_json = json.loads(value)
    return [json.dumps(e) for e in bundle_json['entry']]

  def fhir_bundles_to_omop_cdm(self, path, cdm_database, mapping_database, overwrite):
    
    entries_df=self.load_entries_df(path)
    person_df = entries_to_person(entries_df)
    condition_df = entries_to_condition(entries_df)
    procedure_occurrence_df = entries_to_procedure_occurrence(entries_df)
    encounter_df = entries_to_encounter(entries_df)

    self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {cdm_database}')
    self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {mapping_database}')
    self.spark.catalog.setCurrentDatabase(cdm_database)

    print(f'created {cdm_database} and {mapping_database} databases')

    if overwrite:
      person_df.write.format("delta").mode("overwrite").saveAsTable(PERSON_TABLE)
      condition_df.write.format("delta").mode("overwrite").saveAsTable(CONDITION_TABLE)
      procedure_occurrence_df.write.format("delta").mode("overwrite").saveAsTable(PROCEDURE_OCCURRENCE_TABLE)
      encounter_df.write.format("delta").mode("overwrite").saveAsTable(ENCOUNTER_TABLE)
      print(f'created {PERSON_TABLE,CONDITION_TABLE,PROCEDURE_OCCURRENCE_TABLE} and {ENCOUNTER_TABLE} tables.')

    else:
      person_df.write.format("delta").saveAsTable(PERSON_TABLE)
      condition_df.write.format("delta").saveAsTable(CONDITION_TABLE)
      procedure_occurrence_df.write.format("delta").saveAsTable(PROCEDURE_OCCURRENCE_TABLE)
      encounter_df.write.format("delta").saveAsTable(ENCOUNTER_TABLE)
      print(f'updated {PERSON_TABLE,CONDITION_TABLE,PROCEDURE_OCCURRENCE_TABLE} and {ENCOUNTER_TABLE} tables.')

    return OmopCdm(cdm_database, mapping_database)

  
  def omop_cdm_to_person_dashboard(self, cdm_database, mapping_database):
    self.spark.sql(f'USE {cdm_database}')
    person_df = self.spark.read.table(PERSON_TABLE)
    condition_df = self.spark.read.table(CONDITION_TABLE)
    procedure_occurrence_df = self.spark.read.table(PROCEDURE_OCCURRENCE_TABLE)
    
    encounter_df = self.spark.read.table(ENCOUNTER_TABLE)
    
    condition_summary_df = summarize_condition(condition_df)
    procedure_occurrence_summary_df = summarize_procedure_occurrence(procedure_occurrence_df)
                                    
    encounter_summary_df = summarize_encounter(encounter_df)
    
    return PersonDashboard(
      person_df
      .join(condition_summary_df, 'person_id', 'left')
      .join(procedure_occurrence_summary_df, 'person_id', 'left')
      .join(encounter_summary_df, 'person_id', 'left')
    )
