from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.materialized_view
def provider_efficiency():
  df = spark.read.table('silver_appointment')
  df = df.groupBy('provider_id','provider_name').agg(
    round(avg('waiting_process_time'),2).alias('avg_waiting_process_time'),
    round(avg('rooming_process_time'),2).alias('avg_rooming_process_time'),
    round(avg('check_out_process_time'),2).alias('avg_check_out_process_time'),
    round(avg('chart_close_process_time'),2).alias('avg_chart_close_process_time'),
    round(avg('total_processing_time'),2).alias('avg_total_processing_time'))
  return df


@dp.materialized_view
def clinic_efficiency():
  df = spark.read.table('silver_appointment')
  df = df.groupBy('clinic_code','clinic_name').agg(
    round(avg('waiting_process_time'),2).alias('avg_waiting_process_time'),
    round(avg('rooming_process_time'),2).alias('avg_rooming_process_time'),
    round(avg('check_out_process_time'),2).alias('avg_check_out_process_time'),
    round(avg('chart_close_process_time'),2).alias('avg_chart_close_process_time'),
    round(avg('total_processing_time'),2).alias('avg_total_processing_time'))
  return df

