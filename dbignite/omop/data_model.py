import json
import logging


from abc import ABC, abstractmethod
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.catalog import Database

from dbignite.omop.utils import *
from dbignite.omop.schemas import ENTRY_SCHEMA

from pyspark.sql.functions import *
from pyspark.sql.types import *

PERSON_TABLE = "person"
CONDITION_TABLE = "condition"
PROCEDURE_OCCURRENCE_TABLE = "procedure_occurrence"
ENCOUNTER_TABLE = "encounter"


class DataModel(ABC):
    @abstractmethod
    def summary(self) -> DataFrame:
        ...

    @abstractmethod
    def listDatabases(self) -> Iterable[Database]:
        ...
    
    @abstractmethod
    def update(self) -> None:
        ...

class FhirBundles():
    #
    # Only supporting path representations currently
    #
    def __init__(self, defaultResource = None, **args):
        from pyspark.sql import SparkSession
        self.spark = SparkSession.getActiveSession()
        self.df = None #force lazy evaluation
        if defaultResource is None:
            self.defaultResource = self.asWholeTextfile
        else:
            self.defaultResource = defaultResource
        self.args = args

    #
    # Return json bundles as a DF 
    #
    def loadEntries(self):
        if self.df is None:
            self.df = self.defaultResource(**self.args)
        return self.df

    #
    # ... 
    #
    @staticmethod
    @udf(ArrayType(StringType()))  ## TODO change to pandas_udf
    def _entry_json_strings(value):
        """
        UDF takes raw text, returns the
        parsed struct and raw JSON.
        """
        bundle_json = json.loads(value)
        return [json.dumps(e) for e in bundle_json["entry"]]


    #
    # Read a fhir bundle reasource as a whole text file (1 resource per file)
    # @param path of the json bundles
    def asWholeTextfile(self, path):
        return (
            self.spark.read.text(path, wholetext=True)
              .select(explode(FhirBundles._entry_json_strings("value")).alias("entry_json"))
              .withColumn("entry", from_json("entry_json", schema=ENTRY_SCHEMA))
        ).cache()

    #
    # Read a fhir bundle resource as an inline json value (1 resource per line)
    # 
    def asInlineJson(self, path):
        raise NotImplementedError("TODO...")

    #
    # Read a fhir bundle resource as an inline json value (1 resource per line, static 1 entry per bundle)
    # 
    def asInlineJsonSingleton(self, path):
        return (
            self.spark.read.json(self.spark.read.json(path).rdd.map(lambda x: json.dumps({"entry_json": json.dumps(x.asDict())})))
              .withColumn("entry", from_json("entry_json", schema=ENTRY_SCHEMA))
        ).cache()

    def asStream(self, kwargs):
        raise NotImplementedError("TODO...")

    def listDatabases():
        raise NotImplementedError()

    def summary():
        raise NotImplementedError()
    
    def update(self,path:str) -> None:
        self.path=path

class PersonDashboard(DataModel):
    def __init__(self, df: DataFrame = None):
        self.df = df

    def summary(self):
        return self.df

    def listDatabases(self):
        raise NotImplementedError()
    
    def update(self,df:DataFrame) -> None:
        self.df=df
    
class OmopCdm(DataModel):
    def __init__(self, cdm_database: str, mapping_database: str = None):
        self.cdm_database = cdm_database
        self.mapping_database = mapping_database

    def summary(self) -> DataFrame:
        raise NotImplementedError()

    def listDatabases(self):
        return (self.cdm_database, self.mapping_database)
    
    def update(self,cdm_database: str,mapping_database: str = None):
        self.cdm_database = cdm_database
        self.mapping_database = mapping_database

class Transformer(ABC):
    @abstractmethod
    def loadEntries(self) -> DataFrame:
        ...

    @abstractmethod
    def transform(self) -> DataModel:
        ...

class FhirBundlesToCdm(Transformer):
    def __init__(self, spark = None):
        from pyspark.sql import SparkSession
        self.spark = spark if spark is not None else SparkSession.getActiveSession()
        
    def loadEntries(self):
        pass

    def transform(
            self,
            source: FhirBundles,
            target: OmopCdm,
            overwrite: bool = True,
    ) -> OmopCdm:

        cdm_database = target.cdm_database
        mapping_database=target.mapping_database

        entries_df = source.loadEntries()

        person_df = entries_df.transform(entries_to_person)
        condition_df = entries_df.transform(entries_to_condition)
        procedure_occurrence_df = entries_df.transform(entries_to_procedure_occurrence)
        encounter_df = entries_df.transform(entries_to_encounter)

        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {cdm_database}")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {mapping_database}")
        self.spark.catalog.setCurrentDatabase(cdm_database)

        logging.info(f"created {cdm_database} and {mapping_database} databases")

        if overwrite:
            person_df.write.mode("overwrite").saveAsTable(PERSON_TABLE)
            condition_df.write.mode("overwrite").saveAsTable(
                CONDITION_TABLE
            )
            procedure_occurrence_df.write.mode("overwrite").saveAsTable(
                PROCEDURE_OCCURRENCE_TABLE
            )
            encounter_df.write.mode("overwrite").saveAsTable(
                ENCOUNTER_TABLE
            )
            logging.info(
                f"created {PERSON_TABLE, CONDITION_TABLE, PROCEDURE_OCCURRENCE_TABLE} and {ENCOUNTER_TABLE} tables."
            )

        else:
            person_df.write.saveAsTable(PERSON_TABLE)
            condition_df.write.saveAsTable(CONDITION_TABLE)
            procedure_occurrence_df.write.saveAsTable(
                PROCEDURE_OCCURRENCE_TABLE
            )
            encounter_df.write.saveAsTable(ENCOUNTER_TABLE)
            logging.info(
                f"updated {PERSON_TABLE, CONDITION_TABLE, PROCEDURE_OCCURRENCE_TABLE} and {ENCOUNTER_TABLE} tables."
            )

        target.update(cdm_database, mapping_database)

class CdmToPersonDashboard(Transformer):
    def __init__(self):
        from pyspark.sql import SparkSession
        self.spark = SparkSession.getActiveSession()
        
    def loadEntries(self):
      raise NotImplementedError()
      
    def transform(
            self,
            source: OmopCdm,
            target: PersonDashboard,
            overwrite: bool = True,
    ) -> PersonDashboard:
      
        cdm_database = source.listDatabases()[0]
  
        self.spark.sql(f"USE {cdm_database}")
  
        person_df = self.spark.read.table(PERSON_TABLE)
        condition_df = self.spark.read.table(CONDITION_TABLE)
        procedure_occurrence_df = self.spark.read.table(PROCEDURE_OCCURRENCE_TABLE)
  
        encounter_df = self.spark.read.table(ENCOUNTER_TABLE)
  
        condition_summary_df = condition_df.transform(summarize_condition)
        procedure_occurrence_summary_df = procedure_occurrence_df.transform(
            summarize_procedure_occurrence
        )
  
        encounter_summary_df = encounter_df.transform(summarize_encounter)
        person_dashboard_df  = (
            person_df.join(condition_summary_df, "person_id", "left")
            .join(procedure_occurrence_summary_df, "person_id", "left")
            .join(encounter_summary_df, "person_id", "left")
            )
        target.update(person_dashboard_df)
