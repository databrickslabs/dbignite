import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from .test_base import PysparkBaseTest

class TestWriters(PysparkBaseTest):
    
    def test_encoder(self):
        from dbignite.writer.fhir_encoder import FhirEncoder
        e = FhirEncoder(False, False, lambda x: int(x.strip()))
        assert( e.f("123") == 123 )
        assert( e.f("12a") is None ) 

    def test_encoder_manager(self):
        from dbignite.writer.fhir_encoder import FhirEncoderManager, SchemaDataType, FhirEncoder
        em = FhirEncoderManager()
        assert(em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("1234") == 1234)
        assert(em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("abcdefg") == None)

        #test overrides
        em = FhirEncoderManager(override_encoders = {"Patient.multipleBirthInteger": FhirEncoder(False, False, lambda x: float(x) + 1)})
        assert(em.get_encoder("string", ["Patient", "multipleBirthInteger"]).f("1234") == 1235)
        

    def test_target_datatype(self):
        from dbignite.writer.fhir_encoder import SchemaDataType
        from dbignite.fhir_mapping_model import FhirSchemaModel
        field = "Patient.name.given"
        tgt_schema = FhirSchemaModel.custom_fhir_resource_mapping(field.split(".")[0]).schema(field.split(".")[0])
        result = SchemaDataType.traverse_schema(field.split(".")[1:], tgt_schema)
        assert( result == "array<string>")


    def test_hardcoded_values(self):
        from dbignite.writer.bundler import Bundle
        from dbignite.writer.fhir_encoder import MappingManager, Mapping
        data = self.spark.createDataFrame([('CLM123', 'PAT01', 'COH123'), ('CLM345', 'PAT02', 'COH123')],['CLAIM_ID', 'PATIENT_ID', 'PATIENT_COHORT_NUM'])
        maps = [Mapping('CLAIM_ID', 'Claim.id'), 
		Mapping('PATIENT_COHORT_NUM', 'Patient.identifier.value'),
                Mapping('<url of a hardcoded system reference>', 'Patient.identifier.system', True),
		Mapping('PATIENT_ID', 'Patient.id')]
        m = MappingManager(maps, data.schema)
        b = Bundle(m)
        result = b._encode_df(data).take(2)
        assert(result[0][1].get('Patient').get('identifier')[0].get('system') == '<url of a hardcoded system reference>')


    #   
    # Mapping multiple values into a single array testing
    #
    def test_multiple_resources_to_single_value(self):
        from dbignite.writer.bundler import Bundle
        from dbignite.writer.fhir_encoder import MappingManager, Mapping
        # Create a dummy Dataframe with 2 rows of data
        data = self.spark.createDataFrame([('CLM123', 'Emma', 'Maya'), 
				      ('CLM345', 'E', 'J')],
				     ['CLAIM_ID', 'SECOND_BORN', 'FIRST_BORN'])

        maps = [Mapping('CLAIM_ID', 'Patient.id'), 
		Mapping('FIRST_BORN', 'Patient.name.given'),
	        Mapping('SECOND_BORN', 'Patient.name.given')]

        m = MappingManager(maps, data.schema)
        b = Bundle(m)
        result = b._encode_df(data).take(2)
        assert( result[0][0].get('Patient').get('name')[0].get('given') == ['Maya', 'Emma'])
        assert( result[1][0].get('Patient').get('name')[0].get('given') == ['J', 'E'])
        
