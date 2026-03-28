from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.view
def dim_patient_view():
    df = spark.readStream.table('bronze_patient')
    df = df.withColumn('date_of_birth', to_date(col('date_of_birth'), 'yyyy-MM-dd'))
    df = df.withColumn('sex', when(col('sex') == 'M', 'Male').when(col('sex') == 'F', 'Female').otherwise(col('sex')))
    df = df.dropDuplicates(subset=['patient_id'])
    return df


dp.create_streaming_table('silver_patient')
dp.create_auto_cdc_flow(
  target = "silver_patient",
  source = "dim_patient_view",
  keys = ["patient_id"],
  sequence_by = "patient_id",
  stored_as_scd_type = 1
) 

@dp.view
def dim_location_view():
    df = spark.readStream.table('bronze_location')
    df = df.dropDuplicates(subset=['clinic_code'])
    return df


dp.create_streaming_table('silver_location')
dp.create_auto_cdc_flow(
  target = "silver_location",
  source = "dim_location_view",
  keys = ["clinic_code"],
  sequence_by = "clinic_code",
  stored_as_scd_type = 1
) 

@dp.view
def dim_provider_view():
    df = spark.read.table('bronze_provider')
    df = df.dropDuplicates(subset=['provider_id'])
    return df


dp.create_streaming_table('silver_provider')
dp.create_auto_cdc_from_snapshot_flow(
  target = "silver_provider",
  source = "dim_provider_view",
  keys = ["provider_id"],
  stored_as_scd_type = 2
)

@dp.view
def dim_appointment_view():
    df = spark.readStream.table('bronze_appointment')
    df = df.withColumn('appointment_date', to_date(col('appointment_date'), 'yyyy-MM-dd'))
    df = df.withColumn(
        "appointment_ts",
        to_timestamp(concat_ws(" ", col("appointment_date"), col("appointment_time")), "yyyy-MM-dd HH:mm")
    ).withColumn(
        "check_in_ts",
        to_timestamp(concat_ws(" ", col("appointment_date"), col("check_in_time")), "yyyy-MM-dd HH:mm")
    ).withColumn(
        "rooming_ts",
        to_timestamp(concat_ws(" ", col("appointment_date"), col("rooming_time")), "yyyy-MM-dd HH:mm")
    ).withColumn(
        "check_out_ts",
        to_timestamp(concat_ws(" ", col("appointment_date"), col("check_out_time")), "yyyy-MM-dd HH:mm")
    ).withColumn(
        "chart_close_ts",
        to_timestamp(concat_ws(" ", col("appointment_date"), col("chart_close_time")), "yyyy-MM-dd HH:mm")
    )
    df = df.drop('appointment_time','check_in_time','rooming_time','check_out_time','chart_close_time')
    df = df.withColumn('waiting_process_time', datediff(col('check_in_ts'),col('appointment_ts')))
    df = df.withColumn('rooming_process_time', datediff(col('rooming_ts'),col('check_in_ts')))
    df = df.withColumn('check_out_process_time', datediff(col('check_out_ts'),col('rooming_ts')))
    df = df.withColumn('chart_close_process_time', datediff(col('chart_close_ts'),col('check_out_ts')))
    df = df.withColumn('total_processing_time', datediff(col('chart_close_ts'),col('check_in_ts')))
    df = df.dropDuplicates(subset=['appointment_id'])
    return df


dp.create_streaming_table('silver_appointment')
dp.create_auto_cdc_flow(
  target = "silver_appointment",
  source = "dim_appointment_view",
  keys = ["appointment_id"],
  sequence_by = "appointment_id",
  stored_as_scd_type = 1
)