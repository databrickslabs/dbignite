import json

class Bundle():

    #
    # Create bundles from FHIR resources
    #
    def __init__(self, mm):
        self.mm = mm


    #
    # Return new FHIR resource for each row
    #
    def df_to_fhir(self, df):
        return self._encode_to_json(self._encode_df(df))

    def _encode_df(self, df):
        return (df
            .rdd
            .map(lambda row:
                 list(map(lambda resourceType: self.mm.encode(row, resourceType), self.mm.fhir_resource_list())
            ))
        )
            

    #
    # Given an RDD of rows, return 
    #
    def _encode_to_json(self, rdd):
        return (
            rdd
             .map(lambda row: [self._resource_to_fhir(x) for x in row])
             .map(lambda row: {'resourceType': 'Bundle', 'entry': row})
             .map(lambda row: json.dumps(row))
        )

    
    #
    # Given an encoded row, return a single resource value
    #
    def _resource_to_fhir(self, resource):
        return {'resource': {'resourceType': list(resource.keys())[0], **resource[list(resource.keys())[0]] }}
        
