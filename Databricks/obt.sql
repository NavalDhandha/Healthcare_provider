create or refresh materialized view gold_obt
as 
  select  
    ap.appointment_id,
    ap.provider_id,
    ap.clinic_code,
    ap.patient_id,
    ap.mr_number,
    ap.appointment_date,
    ap.appointment_ts,
    ap.check_in_ts,
    ap.rooming_ts,
    ap.check_out_ts,
    ap.appointment_type,
    ap.chart_close_ts,
    ap.waiting_process_time,
    ap.rooming_process_time,
    ap.check_out_process_time,
    ap.chart_close_process_time,
    ap.total_processing_time,
    ap.provider_name,
    pr.specialty,
    pr.department,
    pr.active_flag,
    pr.__START_AT as provider_start_date,
    pr.__END_AT as provider_end_date,
    pa.patient_name,
    pa.date_of_birth,
    pa.sex,
    pa.insurance_provider,
    ap.clinic_name,
    c.address_line1,
    c.city,
    c.state,
    c.zip_code,
    c.region
  from  
    healthcare_provider.bronze.silver_appointment ap
  left join
    healthcare_provider.bronze.silver_provider pr
  on
    ap.provider_id = pr.provider_id
  left join
    healthcare_provider.bronze.silver_patient pa
  on
    ap.patient_id = pa.patient_id
  left join
    healthcare_provider.bronze.silver_location c
  on
    ap.clinic_code = c.clinic_code
  where pr.__END_AT is null
