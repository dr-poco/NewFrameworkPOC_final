SELECT 
    Anomaly_id,
    rim_no,
    rim_status,
    resident,
    rim_class_code,
    acct_no,
    acct_status,
    acct_class_code,
    user_name,
    staff_email,
    line_mgr_email,
    ethix_create_dt,
    BPM_create_dt,
    Reason,
    control_number,
    anomaly_date,
    bdp_ingestion_id
FROM ${ds_zone_db}.cm_aos_3_anomaly_3_1
--cm_aos_3_1_final_report
WHERE bdp_ingestion_id = CAST(datediff(to_date('${run_date}'), to_date('2019-12-05')) AS BIGINT);
--date_format('${run_date}', 'yyyyMMdd'); 