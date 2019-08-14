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

def readSheetData(service,sheet_id,data_range):
    #service=get_service()
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId = sheet_id, range = data_range).execute()
    values = result.get('values', [])
    headers = values.pop(0)



PROJECT_ID='essence-analytics-dwh'


def refreshGeneric(**kwargs):

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
    SCOPE_TABLE='rtf_unpivot_from_looker' #scope table example is Global_Goals.Consolidate.
    dataset_id='rtf_dev'
    client = bq.Client()
    dataset_ref = client.dataset(dataset_id)
    job_config = bq.LoadJobConfig()
    job_config.autodetect = True
    job_config.skip_leading_rows = 1
    job_config.write_disposition = bq.WriteDisposition.WRITE_TRUNCATE
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bq.SourceFormat.CSV
    uri = "gs://sixty-analytics/RTF_Unpivot"
    load_job = client.load_table_from_uri(
        uri, dataset_ref.table(SCOPE_TABLE),  job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(SCOPE_TABLE))
    print("Loaded {} rows.".format(destination_table.num_rows))


# Define the DAG
dag = DAG('RTF_Unpivot_Refresh', description='Refreshes Unpivot Data From Looker -> GCS To BQ',
          default_args=def_args,
          schedule_interval= "45 4 1 * *",
          start_date=datetime(2019,8,12), catchup=False)



start_dummy_task = DummyOperator(task_id='start_dummy_task', dag=dag)
end_dummy_task = DummyOperator(task_id='end_dummy_task', dag=dag)




def sendToGCS(**kwargs):
    client = bq.Client()

    # Write query results to a new table
    job_config = bq.QueryJobConfig()
    table_ref = client.dataset("rtf_dev").table("RTF_Unpivot_AirflowCopied")
    job_config.destination = table_ref
    job_config.write_disposition = bq.WriteDisposition.WRITE_TRUNCATE

    query_job = client.query(
        'SELECT * FROM `essence-analytics-dwh.rtf_dev.v_unpivot_working` ',
        location='US', # Location must match dataset
        job_config=job_config)
    rows = list(query_job)  # Waits for the query to finish


    # Export table to GCS
    destination_uri = "gs://sixty-analytics/RTF_Unpivot_AirflowCopied"
    dataset_ref = client.dataset("rtf_dev", project="essence-analytics-dwh")
    table_ref = dataset_ref.table("RTF_Unpivot_AirflowCopied")

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location='US')
    extract_job.result()














task = PythonOperator(
task_id = 'RTF_Unpivot_Refresh',
python_callable = refreshGeneric,
dag = dag, provide_context = True)

task2 = PythonOperator(
task_id = 'Export_RTF_Unpivot_Refresh',
python_callable = sendToGCS,
dag = dag, provide_context = True)


start_dummy_task >> task >> task2 >> end_dummy_task
