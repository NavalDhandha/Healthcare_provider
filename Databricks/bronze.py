from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from io import StringIO
import pandas as pd

def load_json(file_name):
    url = f"https://healthcarenaval.blob.core.windows.net/bronze/ingestion/{file_name}.json?sp=rw&st=2026-03-26T05:57:27Z&se=2026-04-01T14:12:27Z&spr=https&sv=2025-11-05&sr=c&sig=EL%2BL3PBs5el8mj%2Fbg3KFBRChaFj6QXWKnzPKZCTjg7Y%3D"

    r = requests.get(url)
    df = pd.read_json(StringIO(r.text),lines=True,convert_dates=False)
    df_spark = spark.createDataFrame(df)
    return df_spark

@dp.table
def bronze_appointment():
    return load_json('appointments')

@dp.table
def bronze_patient():
    return load_json('patient')

@dp.table
def bronze_provider():
    return load_json('provider')

@dp.table
def bronze_location():
    return load_json('location')

