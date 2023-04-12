import logging
import os

# from airflow import DAG
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

from boto3.session import Session

from google.cloud import storage

from datetime import datetime, timedelta

# Past scheduled executions run from this date
start_date = datetime(2023, 3, 29, 0, 0)

# Airflow variables (environment)

retries = int(Variable.get('LOAD_NEWRELIC_TRANSACTIONS_RETRIES'))
retry_delay = int(Variable.get('LOAD_NEWRELIC_TRANSACTIONS_RETRY_DELAY'))

notification_email = Variable.get('COMPOSER_NOTIFICATION_EMAIL')
email_on_failure = Variable.get('LOAD_NEWRELIC_TRANSACTIONS_EMAIL_ON_FAILURE', deserialize_json=True)
email_on_retry = Variable.get('LOAD_NEWRELIC_TRANSACTIONS_EMAIL_ON_RETRY', deserialize_json=True)
email_on_success = Variable.get('LOAD_NEWRELIC_TRANSACTIONS_EMAIL_ON_SUCCESS', deserialize_json=True)

# S3 bucket name, credentials variables
aws_access_key_id = Variable.get('METAPACK_AWS_ACCESS_KEY_ID_NEWRELIC_TRANSACTIONS')
aws_secret_access_key = Variable.get('METAPACK_AWS_SECRET_ACCESS_KEY_NEWRELIC_TRANSACTIONS')
metapack_newrelic_transactions_s3_bucket_name = Variable.get('METAPACK_NEWRELIC_TRANSACTIONS_S3_BUCKET')

# GCS bucket name 
gcs_bucket_name = Variable.get('METAPACK_NEWRELIC_TRANSACTIONS_GCS_BUCKET')

gcs_client = storage.Client()
gcs_bucket = gcs_client.bucket(gcs_bucket_name)

# Airflow default arguments
default_args = {
    'start_date': start_date,
    'email': [notification_email],
    'email_on_failure': email_on_failure,
    'email_on_retry': email_on_retry,
    'retries': retries,
    # 'params': {
    #     'project': bq_project_id
    # },
    'retry_delay': timedelta(minutes=retry_delay)
}

# Upload local file to GCS
def upload_to_gcs(bucket, blob_name, path_to_file):
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

def build_gcs_partition(s3_object_key):
    # TODO add error handling
    gcs_partition = datetime.strptime(s3_object_key[5:15], '%Y/%m/%d').strftime('%Y-%m-%d')
    return gcs_partition

def build_gcs_object_path(gcs_partition, object_name):
    return f'transaction/partition_date={gcs_partition}/{object_name}'

#ingest objects from S3 by date
def ingest_s3_objects(ti, **kwargs):
    # Create AWS sessions and the s3 resource to operate with S3 bucket
    session = Session(aws_access_key_id = aws_access_key_id,
                        aws_secret_access_key = aws_secret_access_key)

    s3 = session.resource('s3')
    s3_bucket = s3.Bucket(metapack_newrelic_transactions_s3_bucket_name)
    s3_date = kwargs['date']

    s3_prefix_base = f'data/{s3_date[0:4]}/{s3_date[4:6]}/{s3_date[6:]}'

    print(f'date = {s3_date}')
    total_object_count = 0
    for i in range(24):
        s3_prefix = f'{s3_prefix_base}/{i:02}'
        total_object_count += copy_from_s3_to_gcs(s3_bucket, s3_prefix)

    ti.xcom_push("total_object_count", total_object_count)
    # ti.xcom_push("test_message", "test of passing context between tasks")
    return f'ingest_s3_object DONE. {total_object_count} objects created on {s3_date} uploaded to GCS'

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/bucket/objects.html

def copy_from_s3_to_gcs(s3_bucket, s3_prefix):
    
    objects = s3_bucket.objects.filter(
        Prefix = s3_prefix
    )
    object_count = 0
    logging.info(f's3_prefix={s3_prefix}')
        
    for s3_object in objects:
        object_count += 1
        object_name = s3_object.key.split('/')[-1]
        gcs_partition = build_gcs_partition(s3_object.key)
        gcs_object_path = build_gcs_object_path(gcs_partition, object_name)       

        s3_bucket.download_file(s3_object.key, object_name)

        upload_to_gcs(bucket = gcs_bucket,
         blob_name = gcs_object_path, 
         path_to_file = object_name)
        
        logging.info(f'File {gcs_object_path} uploaded to GCS')

        os.remove(object_name)

    logging.info(f'{object_count} objects uploaded to GCS')
    if object_count >= 1000:
        logging.error(f'Potentailly not all object read. S3 client has limit of 1000 keys. Change implementation to use Marker param of filter funtion.')
    return object_count    


dag = models.DAG('load_metapack_newrelic_transction_files_s3_to_gcs',
                    description = 'DAG which loads the Transaction events exported by New Relic from an AWS S3 bucket to a Google GCS bucket',
                    schedule_interval = '30 01 * * *',
                    default_args = default_args,
                    max_active_runs = 2,
                    dagrun_timeout=timedelta(hours=8)
                    )


def s3_ingest():
    return PythonOperator(
        task_id = 's3_ingest',
        python_callable = ingest_s3_objects,
        op_kwargs = { 'date' : '{{ ds_nodash }}' },  
        dag = dag)

def send_email():
    return EmailOperator(
        task_id = "send_email",
        conn_id = "sendgrid_default",
        # You can specify more than one recipient with a list.
        to = email_on_success,
        subject = "Load New Relic Transactions to GCS: {{ ds_nodash }}",
        html_content = "<h1>{{task_instance.xcom_pull(task_ids='s3_ingest', key='total_object_count')}} files copied from AWS S3 bucket to GCS bucket</h1>",
        dag=dag,
    )

s3_ingest() >> send_email()

# date = '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}'
#op_kwargs={'date': days_ago(1)},
# op_kwargs = { 'date' : '{{ airflow.macros.ds_format(airflow.macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}' },
        