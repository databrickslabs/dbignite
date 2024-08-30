import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from .test_base import PysparkBaseTest

class TestReaders(PysparkBaseTest):
    def test_FhirFormat(self):
        from dbignite.readers import FhirFormat
        assert (FhirFormat.BUNDLE.name,FhirFormat.BUNDLE.value) == ('BUNDLE', 1)
        assert (FhirFormat.NDJSON.name,FhirFormat.NDJSON.value) == ('NDJSON', 2)
        assert (FhirFormat.BULK.name,FhirFormat.BULK.value) == ('BULK', 3)


    def test_readFromDirectoryBundle(self):
        from dbignite.readers import read_from_directory, FhirFormat
        from  dbignite.fhir_resource import BundleFhirResource
        x = read_from_directory("./sampledata/*json", resource_format = FhirFormat.BUNDLE)
        self.assertTrue(isinstance(type(x), type(BundleFhirResource)))
        self.assertTrue ( x.entry().count() == 3 )
        self.assertTrue([y.asDict()['first'] for y in x.entry().select(col("Patient.name")[0][0].alias("name")).select(col("name.given")[0].alias("first")).collect()].sort() == ['Abe', 'Abraham', 'Abe'].sort() ) 

    def test_readFromDirectoryNDJSON(self):
        from dbignite.readers import read_from_directory, FhirFormat
        from  dbignite.fhir_resource import BundleFhirResource
        x = read_from_directory("./sampledata/ndjson_records/*json", resource_format = FhirFormat.NDJSON)
        self.assertTrue(isinstance(type(x), type(BundleFhirResource)))
        self.assertTrue ( x.entry().count() == 1 )
        
        
    
if __name__ == '__main__':
    unittest.main()
