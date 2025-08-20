SELECT * FROM (
    WITH hash_table AS (
        SELECT *,
            md5(regexp_replace(concat(
                rim_no, rim_status, resident, rim_class_code,
                acct_no, acct_status, acct_class_code,
                user_name, ethix_create_dt, bpm_create_dt, Reason
            ), ' ', '')) AS hash_value
        FROM ${ds_zone_db}.${preprocess_table}
    ),

    anomaly_table AS (
        SELECT DISTINCT anomaly_id, hash_value
        FROM ${ds_zone_db}.${anomaly_table}
    ),

    joined_data AS (
        SELECT has.*,
            anomal.anomaly_id AS existing_anomaly_id
        FROM hash_table AS has
        LEFT JOIN anomaly_table AS anomal
        ON has.hash_value = anomal.hash_value
    ),

    final_result AS (
        SELECT rim_no,
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
            bpm_create_dt,
            Reason as reason,
            hash_value,
            CASE
                WHEN existing_anomaly_id IS NOT NULL THEN existing_anomaly_id
                ELSE concat('3-', date_format(current_timestamp(), 'yyyyMMdd-HHmmssSSS'))
            END AS anomaly_id,
            'control_3' AS control_number,
            date_format(current_timestamp(), 'HHmmss') AS load_hms,
            date_format(current_date(), 'yyyy-MM-dd') AS load_date,
            date_format('${run_date}', 'yyyy-MM-dd') AS anomaly_date,
            CAST(datediff(to_date('${run_date}'), to_date('2019-12-05')) AS BIGINT) AS bdp_ingestion_id
        FROM joined_data
    )

    SELECT * FROM final_result
) result;
