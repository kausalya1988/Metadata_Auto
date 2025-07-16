import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import boto3
import json
from botocore.exceptions import ClientError
 
 
def get_secret(secret_name):
 
    region_name = "eu-west-3"
 
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
 
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        raise e
 
    secret = get_secret_value_response['SecretString']
    return secret
 
 
def get_ssm_parameter(parameter_name, with_decryption=False):
    # Create the SSM client
    ssm_client = boto3.client('ssm')
 
    try:
        # Get the parameter value
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=with_decryption  # Set to True if retrieving a SecureString
        )
        # Access the parameter value
        parameter_value = response['Parameter']['Value']
        return parameter_value
    except Exception as e:
        print(f"Error retrieving parameter: {e}")
        return None 


#define variable
snowflake_secret = "sec-dlz-ENVIRONMENT-${jv}-snowflake-password"
snowflake_account_ssm = "ssp-dlz-ENVIRONMENT-snowflake-account"
snowflake_user_ssm = "ssp-dlz-ENVIRONMENT-${jv}-snowflake-user"
snowflake_role_ssm = "ssp-dlz-ENVIRONMENT-${jv}-snowflake-role"
snowflake_warehouse_ssm = "ssp-dlz-ENVIRONMENT-${jv}-snowflake-warehouse"

SNOWFLAKE_ACCOUNT = get_ssm_parameter(snowflake_account_ssm)
SNOWFLAKE_USER = get_ssm_parameter(snowflake_user_ssm)
SNOWFLAKE_ROLE = get_ssm_parameter(snowflake_role_ssm)
SNOWFLAKE_WAREHOUSE = get_ssm_parameter(snowflake_warehouse_ssm)
SNOWFLAKE_PASSWORD = get_secret(snowflake_secret)

# Fonction pour extraire la balise date_batch de la notification SQS
def extract_date_batch(**kwargs):
    notification = kwargs['ti'].xcom_pull(task_ids='wait_for_sqs_message', key='messages')
    body = notification[0]['Body']
    message = json.loads(body)
    date_batch = message['dateBatch']
    return date_batch
 


# STEP 01 - Define the task to check files in AWS Glue
def check_glue_file_structure(**kwargs):
    """
    This function pulls an SQS message from XCom, extracts the body,
    and triggers an AWS Glue job to verify the file structure.
    """
    # Retrieve the task instance and the SQS message from XCom
    task_instance = kwargs['ti']
    message = task_instance.xcom_pull(task_ids='wait_for_sqs_message', key='messages')
 
    if message:
        # Extract the body of the first message
        body = message[0]['Body']
        # Parse the JSON content from the message body
        message_data = json.loads(body)
 
        # Set the Glue job name and prepare arguments
        glue_job_name = "glue-job-dlz-ENVIRONMENT-${project}-${source_split}-${jv}-s3sc${layout}"
        job_arguments = {
            '--notification': json.dumps(message_data)
        }
 
        try:
            # Trigger the AWS Glue job
            glue_job_operator = GlueJobOperator(
                task_id='run_glue_file_structure_check_op',
                job_name=glue_job_name,
                script_args=job_arguments,
                aws_conn_id='aws_default',
                region_name='eu-west-3',
                dag=kwargs['dag'],
            )
            glue_job_operator.execute(context=kwargs)
       
        except Exception as e:
            logging.error(f"Glue Job failed: {e}")
            return 'end_workflow'
    else:
        # Raise an exception if no message is found in XCom
        logging.warning("No message found in XCom.")
        return 'end_workflow'
 
 
# STEP 02 - Define the task to convert CSV to Parquet using AWS Glue
def convert_csv_to_parquet(**kwargs):
    """
    This function triggers an AWS Glue job to convert CSV files to Parquet format.
    It retrieves the SQS message from XCom and passes it as a parameter to the Glue job.
    """
    # Retrieve the task instance and the SQS message from XCom
    task_instance = kwargs['ti']
    message = task_instance.xcom_pull(task_ids='wait_for_sqs_message', key='messages')
 
    if message:
        # Extract and parse the message body
        body = message[0]['Body']
        message_data = json.loads(body)
 
        # Set the Glue job name and prepare arguments
        glue_job_name = "glue-job-dlz-ENVIRONMENT-${project}-${source_split}-${jv}-ctop"
        job_arguments = {
            '--notification': json.dumps(message_data)
        }
 
        try:
            # Trigger the AWS Glue job
            glue_job_operator = GlueJobOperator(
                task_id='run_csv_to_parquet_conversion_op',
                job_name=glue_job_name,
                script_args=job_arguments,
                aws_conn_id='aws_default',
                region_name='eu-west-3',
                dag=kwargs['dag'],
            )
            glue_job_operator.execute(context=kwargs)
        except Exception as e:
            logging.error(f"Glue Job failed: {e}")
            return 'end_workflow'
    else:
        # Raise an exception if no message is found in XCom
        logging.warning("No message found in XCom.")
        return 'end_workflow'
 
 
# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1, 0, 0),
    'retries': 0,
    'max_active_runs': 1
}
 
# Define the DAG
dag = DAG(
    dag_id='${jv}_el_${source}_${period}',
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered manually or by an event
    catchup=False,
    tags = ['el','${jv}','${source}','${period}']
)
# Dummy task to indicate the Start of the workflow
start = DummyOperator(
    task_id='start',
    dag=dag
)

# SQS Sensor: Waits for a message in the SQS queue
wait_for_sqs_message = SqsSensor(
    task_id='wait_for_sqs_message',
    sqs_queue='SQS_QUEUE_URL',  # Your SQS Queue URL
    aws_conn_id='aws_default',  # AWS connection configured in Airflow
    max_messages=1,  # Retrieve one message
    message_filtering='jsonpath',
    message_filtering_config='$$.project',
    message_filtering_match_values=['${project}'],
    #wait_time_seconds=10,  # Time to wait between checks
    poke_interval= 5 * 60,  # How often to poll the SQS queue (5 minutes)
    #visibility_timeout=600,  # SQS message visibility timeout
    timeout= 1 * 60 * 60,  # Timeout for the sensor (1  heure)
    mode='reschedule',  # 'reschedule' mode periodically checks the queue
    dag=dag
)

# Task pour extraire la balise date_batch
extract_date_batch_task = PythonOperator(
    task_id='extract_date_batch',
    python_callable=extract_date_batch,
    provide_context=True,
    dag=dag,
) 

# Task to run Glue job for file structure check
run_glue_file_structure_check = PythonOperator(
    task_id='run_glue_file_structure_check',
    python_callable=check_glue_file_structure,
    provide_context=True,
    dag=dag
)
 
 
# Task to run Glue job for CSV to Parquet conversion
run_csv_to_parquet_conversion = PythonOperator(
    task_id='run_csv_to_parquet_conversion',
    python_callable=convert_csv_to_parquet,
    provide_context=True,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"source /usr/local/airflow/python3-virtualenv/dbt-env/bin/activate;\
    cp -R /usr/local/airflow/dags/${jv}-el-${source_file}-${period}/dbt/ /tmp;\
    ls -R /tmp;\
    cd /tmp/dbt/;\
    /usr/local/airflow/python3-virtualenv/dbt-env/bin/dbt deps;\
    /usr/local/airflow/python3-virtualenv/dbt-env/bin/dbt run-operation refresh_external_tables_model_${source};\
    /usr/local/airflow/python3-virtualenv/dbt-env/bin/dbt test --vars '{{{{ var('date_variable', \'{{ ti.xcom_pull(task_ids='extract_date_batch') }}\') }}' ;\
    rm -rf /tmp/dbt/",
    env={'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_PASSWORD': SNOWFLAKE_PASSWORD,
        'SNOWFLAKE_ROLE': SNOWFLAKE_ROLE,
        'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE,
        'ENV_DBT': 'ENVIRONMENT'},
    dag=dag
)


# SNS Publish operator to publish message to SNS topic after the upward tasks are successful
send_sns = SnsPublishOperator(
    task_id='send_sns_message',
    target_arn=SNS_QUEUE_URL,  # SNS topic arn to which you want to publish the message
    message= "{{ ti.xcom_pull(task_ids='wait_for_sqs_message', key='messages') }}", # To completed
    subject='Message to Snowflake team ${source_file}-${period}',
    dag=dag
)


# Dummy task to indicate the success of the workflow
end_successfull = DummyOperator(
    task_id='end_successfull',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)


# Dummy task to indicate the end of the workflow
end_error = DummyOperator(
    task_id='end_error',
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)
 
# Define the task dependencies
start >> wait_for_sqs_message >> extract_date_batch_task >> run_glue_file_structure_check >> run_csv_to_parquet_conversion >> dbt_run >> send_sns
[extract_date_batch_task, run_glue_file_structure_check, run_csv_to_parquet_conversion, dbt_run, send_sns] >> end_successfull
[extract_date_batch_task, run_glue_file_structure_check, run_csv_to_parquet_conversion, dbt_run, send_sns] >> end_error
