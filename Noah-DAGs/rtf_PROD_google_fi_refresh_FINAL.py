from airflow import DAG
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import date, timedelta, datetime
import logging
from google.cloud import bigquery as bq
from googleapiclient import discovery
from google.oauth2 import service_account
import pandas as pd
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import re


from dateutil.relativedelta import relativedelta

import os
import sys
from datetime import datetime, timedelta, date
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
import numpy as np


#Don't edit any of this, it is good the way it is.

from google.oauth2 import service_account
import json
import base64
from google.cloud import kms_v1
from google.cloud import storage as gcsStorage



def_args = {
    'owner': 'Noah Hazan',
    'depends_on_past': False,
    'email': ['noah.hazan@essenceglobal.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'execution_timeout': timedelta(minutes=10),
    'task_concurrency': 1,
    'retry_delay': timedelta(minutes=2)
}


def fetch_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = gcsStorage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob_data=blob.download_as_string()
    return blob_data

def decrypt_symmetric(encrypted_text):
    """Decrypts input ciphertext using the provided symmetric CryptoKey."""

    # crypto key params
    project_id='essence-analytics-etl-207615'
    location_id='global'
    key_ring_id='TeamAnalytics'
    crypto_key_id='SecureAnalytics'
    # Creates an API client for the KMS API.
    client = kms_v1.KeyManagementServiceClient()
    response=''
    # The resource name of the CryptoKey.
    name = client.crypto_key_path_path(project_id, location_id, key_ring_id,
                                       crypto_key_id)
    # Use the KMS API to decrypt the data.

    ciphertext=base64.b64decode(encrypted_text)
    response = json.loads(client.decrypt(name, ciphertext).plaintext)
    return response

# Use This function to fetch your credentials. Don't store credentials. Keys could be deleted anytime due to security issue.
# You should have logic to regenrate credentials if you encounter google.auth.exceptions.RefreshError
def getCredentialsFromVault(service_account_email):
    StorageBucket='essence-analytics-5a9706e0-d22d-40f3-abde-2a996137bb0f'
    blob_name=service_account_email+'.json.enc'
    enc_key=fetch_blob(StorageBucket, blob_name)
    credentials = service_account.Credentials.from_service_account_info(decrypt_symmetric(enc_key))
    print(credentials)
    return credentials

PROJECT_ID='essence-analytics-dwh'

query = ['select * from `essence-analytics-dwh.rtf_google_fi.STAGE_google_fi_VIEW`', 'select * from `essence-analytics-dwh.rtf.rtf_STAGE_google_fi_device_fires_VIEW`', 'select * from `essence-analytics-dwh.rtf_google_fi.STAGE_week_deltas_VIEW`']
destination =['rtf_PROD_google_fi_datastudio_FINAL','rtf_PROD_google_fi_datastudio_device_fires_FINAL','rtf_PROD_google_fi_datastudio_week_deltas_FINAL']


query_destination = dict(zip(query, destination))





def staging_to_prod(**kwargs):

    API_SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
              'https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/cloud-platform']

	#service e-mail.
    service_account_email='data-strategy@essence-analytics-dwh.iam.gserviceaccount.com'

	#On Google sheets, share this e-mail on the google sheets page you want to pull data from.
    credentialsFromVault=getCredentialsFromVault(service_account_email)
    credentialsFromVault = credentialsFromVault.with_scopes(API_SCOPES)
    sheet_service = build('sheets', 'v4', credentials=credentialsFromVault,cache_discovery=False)
    client = bq.Client()

    # Write query results to a new table
    job_config = bq.QueryJobConfig()
    table_ref = client.dataset("rtf_google_fi").table(kwargs['destination'])
    job_config.destination = table_ref
    job_config.write_disposition = bq.WriteDisposition.WRITE_TRUNCATE

    query_job = client.query(
        kwargs['query'],
        location='US', # Location must match dataset
        job_config=job_config)
    rows = list(query_job)  # Waits for the query to finish



# Define the DAG
dag = DAG('rtf_PROD_google_fi_refresh_FINAL', description='Refreshes all datasources in Google Fi',
          default_args=def_args,
          schedule_interval= "45 4 1 * *",
          start_date=datetime(2019,8,20), catchup=False)



start_dummy_task = DummyOperator(task_id='start_dummy_task', dag=dag)
end_dummy_task = DummyOperator(task_id='end_dummy_task', dag=dag)


for query, destination in query_destination.items():
    task = PythonOperator(
    task_id = destination + '_refresh',
    python_callable = staging_to_prod,
    op_kwargs = {
        'query':query,
        'destination':destination},
    dag = dag, provide_context = True)

    start_dummy_task >> task >> end_dummy_task
