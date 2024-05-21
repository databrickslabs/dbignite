# pyspark --master local[8] --driver-memory 8G

from  dbignite.fhir_mapping_model import FhirSchemaModel
from pyspark.sql.functions import size,col, sum, when, explode, udf, flatten, expr, to_json, transform, concat
from pyspark.sql.types import * 
import uuid, random
from dbignite.readers import read_from_directory
sample_data = "./sampledata/*json"

bundle = read_from_directory(sample_data)
df = bundle.entry()

test_rec_p = df.select("Patient").limit(1)
#toJSON() working with 8GB of memory
test_rec_p.toJSON().first()
test_rec_p.select(to_json(test_rec_p.Patient)).show()


#Convert each element of the array to a String
test_rec_p.select(transform("Patient", lambda x: to_json(x)).alias("json_str")).printSchema()
test_rec_c = df.select("Claim").limit(1)
test_rec_c.select(size("Claim")).show()
test_rec_c.select(transform("Claim", lambda x: to_json(x)).alias("json_str")).printSchema()


#Concat 2 columns...
df.select(concat(transform("Patient", lambda x: to_json(x)), transform("Claim",  lambda x: to_json(x))).alias("entry_payload")).select(size("entry_payload")).show()

#Dynamically concat all columns together into Entry array 
df.select(concat(*[transform(x, lambda y: to_json(y)) for x in df.columns]).alias("json_payload")).show()
( df.select(concat(*[transform(x, lambda y: to_json(y)) for x in df.columns]).alias("json_payload"))
  .select(size("json_payload")).show()
 )

#Write & save everything off as its own table 

#
# parallel writes to a table 
#
def copyWrite(df, colName):
    df.select(colName).write.mode("append").parquet(str(colName) +".parquet")

from multiprocessing.pool import ThreadPool
import multiprocessing as mp
pool = ThreadPool(mp.cpu_count()-1)

import time
start = time.time()
list(pool.map(lambda x: copyWrite(df, x), df.columns))
end = time.time()
print(end - start)

