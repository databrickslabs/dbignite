from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession

"""
 Referencing actions from https://pubs.vocera.com/vcts/vcts_2.5.1/help/vcts_config_help/topics/vcts_listofeventtypes.html
"""

class ADTActions:
    def __init__(self):
        try:
            self.register_udf(spark = SparkSession.getActiveSession())
        except Exception as e:
            print("WARN: dbignite is not registering ADT actions as a Spark UDF due to")
            print("WARN: " + str(e))

    @staticmethod
    def adt_msg():
        return {
            "ADT_A01": { "action": "admit", "description": "admit a patient"},
            "ADT_A02": { "action": "transfer", "description": "transfer a patient"},
            "ADT_A03": { "action": "discharge", "description": "discharge a patient"},
            "ADT_A04": { "action": "admit", "description": "register a patient"},
            "ADT_A05": { "action": "admit", "description": "preadmit a patient"},
            "ADT_A06": { "action": "admit", "description": "transfer an outpatient to inpatient"},
            "ADT_A07": { "action": "discharge", "description": "transfer an inpatient to outpatient"},
            "ADT_A08": { "action": "", "description": "update patient information"},
            "ADT_A09": { "action": "discharge", "description": "patient departing"},
            "ADT_A10": { "action": "", "description": "patient arriving"},
            "ADT_A11": { "action": "discharge", "description": "cancel admit"},
            "ADT_A12": { "action": "", "description": "cancel transfer"},
            "ADT_A13": { "action": "", "description": "cancel discharge"},
            "ADT_A14": { "action": "", "description": "pending admit"},
            "ADT_A15": { "action": "", "description": "pending transfer"},
            "ADT_A16": { "action": "", "description": "pending discharge"},
            "ADT_A17": { "action": "transfer", "description": "swap patients"},
            "ADT_A18": { "action": "", "description": "merge patient information"},
            "ADT_A19": { "action": "", "description": "patient, query"},
            "ADT_A20": { "action": "", "description": "nursing/census application updates"},
            "ADT_A21": { "action": "", "description": "leave of absence - out (leaving)"},
            "ADT_A22": { "action": "", "description": "leave of absence - in (returning)"},
            "ADT_A23": { "action": "", "description": "delete a patient record"},
            "ADT_A24": { "action": "", "description": "link patient information"},
            "ADT_A25": { "action": "", "description": "cancel pending discharge"},
            "ADT_A26": { "action": "", "description": "cancel pending transfer"},
            "ADT_A27": { "action": "", "description": "cancel pending admit"},
            "ADT_A28": { "action": "", "description": "add person information"},
            "ADT_A29": { "action": "", "description": "delete person information"},
            "ADT_A30": { "action": "", "description": "merge person information"},
            "ADT_A31": { "action": "", "description": "update person information"},
            "ADT_A32": { "action": "", "description": "cancel patient arriving"},
            "ADT_A33": { "action": "", "description": "cancel patient departing"},
            "ADT_A34": { "action": "", "description": "merge patient information - patient id only"},
            "ADT_A35": { "action": "", "description": "merge patient information - accounting number only"},
            "ADT_A36": { "action": "", "description": "merge patient information - patient id and accounting number"},
            "ADT_A37": { "action": "", "description": "unlink patient information"}
        }
        

    #
    # Given a python dictionary, return the relevant ADT information
    #  data = json.load(open("sampledata/adt_records/ADT_A01_FHIR.json", 'rb'))
    #
    @staticmethod
    def get_action_from_bundle(json_fhir_bundle):
        return [ADTActions.adt_msg().get(x.get("resource").get("eventCoding").get("code")) for x in data.get("entry") if x.get("resource").get("resourceType") == "MessageHeader"][0]

    @staticmethod
    def get_action(action):
        return ADTActions.adt_msg().get(action, {"action":"", "description":""})

    #register as spark udf
    @staticmethod
    def register_udf(spark = SparkSession.getActiveSession(), udf_name = "get_action"):
        schema = StructType([
            StructField("action", StringType(), False),
            StructField("description", StringType(), False)
        ])
        spark.udf.register(udf_name, udf(ADTActions.get_action, schema))    
        
