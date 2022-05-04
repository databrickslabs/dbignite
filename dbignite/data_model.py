from abc import ABC, abstractmethod
from enum import Enum
from typing import Iterable, Type

from pyspark.sql import DataFrame
from pyspark.sql.catalog import Database


class DataModel(ABC):
  
  @abstractmethod
  def summary(self) -> DataFrame:
    ...
    
  @abstractmethod
  def listDatabases(self) -> Iterable[Database]:
    ...


class FhirBundles(DataModel):
  
  def __init__(self, path: str, cdm_database : str, cdm_mapping_database: str):
    self.path = path
    self.cdm_database = cdm_database
    self.cdm_mapping_database = cdm_mapping_database
    
  def listDatabases():
    raise NotImplementedError() #TODO change to list database
    
  def summary():
    raise NotImplementedError() 


class PersonDashboard(DataModel):
  
  @classmethod
  def builder(cls, from_: DataModel):
    if isinstance(from_, FhirBundles):
      return cls._from_fhir_bundles(from_)
    else:
      raise NotImplementedError()
  
  def __init__(self, df: DataFrame):
    self.df = df
    
  def summary(self):
    return self.df
  
  def listDatabases(self):
    raise NotImplementedError('TODO: persist the person dashboard')
    
  @staticmethod
  def _from_fhir_bundles(from_: FhirBundles):
    omop_cdm = fhir_bundles_to_omop_cdm(from_.path,from_.cdm_database,from_.cdm_mapping_database)
    person_dashboard = omop_cdm_to_person_dashboard(*omop_cdm.listDatabases())
    return person_dashboard


class OmopCdm(DataModel):
  
  def __init__(self, cdm_database: str, mapping_database: str):
    self.cdm_database = cdm_database
    self.mapping_database = mapping_database
    
  def summary(self) -> DataFrame:
    raise NotImplementedError('TODO: summarize OMOP CDM in a DataFrame')
  
  def listDatabases(self):
    return (self.cdm_database, self.mapping_database)