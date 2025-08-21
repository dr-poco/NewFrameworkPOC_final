from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from dateutil import parser
from airflow.exceptions import AirflowException
import jsonschema
from jsonschema import validate, ValidationError
import json
from collections import defaultdict

spark_variables = Variable.get("dag_dpf__cm_aos_3", deserialize_json=True)
json_schema = Variable.get("json_schema", deserialize_json=True)
global_variables = Variable.get("dpf_cm_global_variables", deserialize_json=True)

precheck_command = spark_variables["control_variables"]["precheck_command"]
ssh_connection_informatica = str(Variable.get("ssh_connection_informatica"))

schedule = spark_variables["control_variables"]["schedule"]
# Get dag_alert_list from control_variables first, fall back to global_variables if not found
dag_alert_list = spark_variables["control_variables"].get("dag_alert_list", global_variables.get("dag_alert_list", ""))
dag_alert_list = str(dag_alert_list)

# Split email addresses by semicolon and strip whitespace
email_list = [email.strip() for email in dag_alert_list.split(";") if email.strip()]

default_args = {
    "owner": "ADIB_COLLAB_AI",
    "email": email_list,
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    "start_date": parser.isoparse("2025-01-01T00:00:00Z").replace(tzinfo=timezone.utc),
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    "provide_context": True,
    "wait_for_downstream": False
}

dag = DAG(
    "dpf_cm_aos_control_3_dag",
    concurrency=spark_variables["control_variables"].get("concurrency", 3),
    schedule_interval=schedule,
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=True,
    tags=[tag.strip() for tag in spark_variables["control_variables"]["tags"].split(",")] if spark_variables["control_variables"]["tags"] else [],
    max_active_runs=1
)

def validate_json_schema(json_data, json_schema):
    try:
        validate(instance=json_data, schema=json_schema)
        print("Validation passed")
        
        # Custom validation for precheck_command
        if "control_variables" in json_data:
            control_vars = json_data["control_variables"]
            dag_name = control_vars.get("dag_name")
            precheck_cmd = control_vars.get("precheck_command")
            
            if dag_name and precheck_cmd:
                # Check if precheck_command ends with dag_name/dag_name_env.sh
                expected_suffix = f"{dag_name}/{dag_name}_env.sh"
                if not precheck_cmd.endswith(expected_suffix):
                    raise AirflowException(f"Precheck command validation failed: '{precheck_cmd}' must end with '{expected_suffix}'")
                print(f"Precheck command validation passed: ends with {expected_suffix}")
        
    except ValidationError as ve:
        raise AirflowException(f"JSON Schema validation failed: {ve.message}")

start = DummyOperator(task_id="start", dag=dag)

validate_json_task = PythonOperator(
    task_id="validate_json",
    dag=dag,
    python_callable=validate_json_schema,
    provide_context=True,
    op_kwargs={
        "json_data": spark_variables,
        "json_schema": json_schema
    }
)

check_table_load = SSHOperator(
    task_id='check_table_load',
    dag=dag,
    ssh_conn_id=ssh_connection_informatica,
    command= precheck_command,
    retries=5
)

validate_json_task >> check_table_load

prev_group = [check_table_load]
# Helper function to process any job section
def process_jobs(job_list, job_type):
    tasks_by_dag_task_order = defaultdict(list)
    for idx, job in enumerate(job_list):
        dag_task_order = job.get("dag_task_order", 99)
        task_id = job.get("job_name")
        spark_submit_config = job.get("spark_submit_config", {})
        job_name = job.get("spark_job_name")

        # Prepare job arguments for CDE configuration tab visibility
        job_args = []
        
        # Add control_variables as individual conf arguments
        for key, value in spark_variables['control_variables'].items():
            job_args.append(f"--conf=spark.custom_conf.control_variables.{key}={json.dumps(value)}")
        
        # Add job_variables as individual conf arguments (excluding spark_submit_config)
        job_vars_to_print = {k: v for k, v in job.items() if k != 'spark_submit_config'}
        for key, value in job_vars_to_print.items():
            job_args.append(f"--conf=spark.custom_conf.job_variables.{key}={json.dumps(value)}")
        
        # Add global_variables as individual conf arguments
        for key, value in global_variables.items():
            job_args.append(f"--conf=spark.custom_conf.global_variables.{key}={json.dumps(value)}")
        
        # Add log_table
        job_args.append("--conf=spark.custom_conf.log_table=ds_dashboard.dpf_processing_log")
        
        # Add any additional spark_submit_config arguments
        if "args" in spark_submit_config:
            job_args.extend(spark_submit_config["args"])
        
        operator = CDEJobRunOperator(
            task_id=task_id,
            dag=dag,
            job_name=job_name,
            overrides={
                "spark": {
                    **spark_submit_config,
                    "conf": {
                        **spark_submit_config.get("conf", {}),
                        "spark.custom_conf.control_variables": json.dumps(spark_variables["control_variables"]),
                        "spark.custom_conf.job_variables": json.dumps(job),
                        "spark.custom_conf.global_variables": json.dumps(global_variables),
                        "spark.custom_conf.log_table": "ds_dashboard.dpf_processing_log"
                    },
                    "args": job_args
                }
            },
            trigger_rule="all_success"
        )
        tasks_by_dag_task_order[dag_task_order].append(operator)

    # Chain tasks group by group
    global prev_group
    for dag_task_order in sorted(tasks_by_dag_task_order.keys()):
        current_group = tasks_by_dag_task_order[dag_task_order]
        for prev_task in prev_group:
            for curr_task in current_group:
                prev_task >> curr_task
        prev_group = current_group

# Process sql_jobs
process_jobs(
    spark_variables.get("sql_jobs", []),
    job_type="sql"
)

# Process normal_email_reports
process_jobs(
    spark_variables.get("normal_email_reports", []),
    job_type="email"
)

# Process user_specific_email_reports
process_jobs(
    spark_variables.get("user_specific_email_reports", []),
    job_type="user_email"
)

end = DummyOperator(task_id="end", dag=dag)

for task in prev_group:
    task >> end
