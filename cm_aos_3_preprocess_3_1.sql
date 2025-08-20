select distinct a.rim_no,
a.rim_status,
a.resident,
a.rim_class_code,
a.acct_no,
a.acct_status,
a.acct_class_code,
cast(a.user_name as string) as user_name,
'karthikeyan.nethaji@adib.com' as staff_email,
'sanjana.krishnan@adib.com' as line_mgr_email,
cast (a.ethix_create_dt as string) as ethix_create_dt ,
cast (a.bpm_create_dt as string) as BPM_create_dt ,
a.Reason as Reason,
CAST(datediff(to_date('${run_date}'), to_date('2019-12-05')) as BIGINT) bdp_ingestion_id from 
(
    select dp.rim_no, rm.rim_status, rm.resident, rm.rim_class_code, dp.acct_no, dp.status as
    acct_status, dp.class_code as acct_class_code, hs.user_name, hs.create_dt as ethix_create_dt,
    null as BPM_create_dt,
    case when su.account_numbers is null and hs.channel_id = 558
    then 'account is closed in ethix but corresponing entry is missing in BPM system'
    end as Reason
            from
        (select rim_no, acct_no, status, class_code, create_dt, bdp_partition_id
        from ${stand_zone_db}.phx_dp_acct
        where cast(substr(bdp_partition_id,3) as DOUBLE)
        between datediff(date_sub(to_date('${run_date}'), ${no_of_past_days} - 1), to_date('2019-12-05')) 
        and  datediff('${run_date}', to_date('2019-12-05'))
        and acct_no in (select acct_no from
        (select acct_no, min(bdp_ingestion_id) = max(bdp_ingestion_id) as ft
        from ${stand_zone_db}.phx_dp_acct
        where cast(substr(bdp_partition_id,3) as DOUBLE)
        between datediff(date_sub(to_date('${run_date}'), ${no_of_past_days} - 1), to_date('2019-12-05')) 
        and  datediff('${run_date}', to_date('2019-12-05')) 
        and status = 'Closed'
        group by acct_no, status) as t
        where ft = true) and bdp_partition_id = concat('P_', CAST(datediff('${run_date}', to_date('2019-12-05')) AS STRING))) as dp
        left join
        (select t1.class_code as rim_class_code, t1.country_code as resident, t1.status as rim_status, t1.rim_no as rim_no
        from (select class_code, status, country_code, rim_no, row_number() over ( partition by rim_no
        order by unix_timestamp(bdp_load_date) desc) as row_number from ${stand_zone_db}.phx_rm_acct) t1
        where t1.row_number<=1) as rm on rm.rim_no=dp.rim_no
        left JOIN
        (SELECT acct_no, user_name, channel_id, create_dt
        from ${stand_zone_db}.phx_atm_tran_log
        where cast(substr(bdp_partition_id,3) as DOUBLE)
        between datediff(date_sub(to_date('${run_date}'), ${no_of_past_days} - 1), to_date('2019-12-05')) 
        and  datediff('${run_date}', to_date('2019-12-05'))  
        and phoenix_trancode = 176 and channel_id = 558 and return_code = 0) as hs
        on dp.acct_no = hs.acct_no
        left join
        (select creation_date, last_activity_date, initiator_id, rim, account_numbers, request_status, app_acronym, app_reference_number
        from ${stand_zone_db}.bpm_dt_bpm_requests_summary
        where cast(substr(bdp_partition_id,3) as DOUBLE)
        between datediff(date_sub(to_date('${run_date}'), ${no_of_past_days} - 1), to_date('2019-12-05')) 
        and  datediff('${run_date}', to_date('2019-12-05')) 
        and app_acronym='ADIBAC' and request_status='Completed') as su
        on dp.acct_no=substring(su.account_numbers,1,8)
        where su.account_numbers is null and hs.channel_id = 558

    union

    select dp.rim_no, rm.rim_status, rm.resident, rm.rim_class_code, dp.acct_no, dp.status as
    acct_status, dp.class_code as acct_class_code, null as user_name,
    null as ethix_create_dt, creation_date as BPM_create_dt,
    case when dp.acct_no != 'Closed'
    then 'account closure request is approved in BPM but corresponing entry is missing in Ethix'
    end as Reason

    from

    (select creation_date, last_activity_date, initiator_id, rim, account_numbers, request_status, app_acronym, app_reference_number
    from ${stand_zone_db}.bpm_dt_bpm_requests_summary where app_acronym='ADIBAC' and request_status='Completed' and bdp_partition_id = concat('P_', CAST(datediff('${run_date}', to_date('2019-12-05')) AS STRING))) as su
    left join
    (select rim_no, acct_no, status, class_code, create_dt, bdp_partition_id
    from ${stand_zone_db}.phx_dp_acct where bdp_partition_id = concat('P_', CAST(datediff('${run_date}', to_date('2019-12-05')) AS STRING))) as dp    -- take the latest partition id
    on dp.acct_no=substring(su.account_numbers,1,8)
    left join
    (select t1.class_code as rim_class_code, t1.country_code as resident, t1.status as rim_status, t1.rim_no as rim_no
    from (select class_code, status, country_code, rim_no, row_number() over ( partition by rim_no
    order by unix_timestamp(bdp_load_date) desc) as row_number from ${stand_zone_db}.phx_rm_acct) t1
    where t1.row_number<=1) as rm on rm.rim_no=dp.rim_no
    where status != 'Closed' ) a