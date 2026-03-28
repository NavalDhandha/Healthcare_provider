from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.materialized_view
def provider_efficiency():
  df = spark.read.table('silver_appointment')
  df = df.groupBy('provider_id','provider_name').agg(
    avg('waiting_process_time').alias('avg_waiting_process_time'),
    avg('rooming_process_time').alias('avg_rooming_process_time'),
    avg('check_out_process_time').alias('avg_check_out_process_time'),
    avg('chart_close_process_time').alias('avg_chart_close_process_time'),
    avg('total_processing_time').alias('avg_total_processing_time'))
  return df


@dp.materialized_view
def clinic_efficiency():
  df = spark.read.table('silver_appointment')
  df = df.groupBy('clinic_code','clinic_name').agg(
    avg('waiting_process_time').alias('avg_waiting_process_time'),
    avg('rooming_process_time').alias('avg_rooming_process_time'),
    avg('check_out_process_time').alias('avg_check_out_process_time'),
    avg('chart_close_process_time').alias('avg_chart_close_process_time'),
    avg('total_processing_time').alias('avg_total_processing_time'))
  return df

