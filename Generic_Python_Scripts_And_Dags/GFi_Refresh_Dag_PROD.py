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

    # Save object to Pandas Dataframe
    df = pd.DataFrame.from_records(values, columns = headers)
    return df


PROJECT_ID='essence-analytics-dwh'
sheet_id = "1cefjnQazJxC9gbrPK1IxETedqQDTvPdV4WdsEykZARY"
source = ['Data | Bing SEM!A2:L367','Data | Brand SEM!A2:H367','Data | Non-Brand BVOS SEM!A2:H367','Data | GDN!A2:J367','Data | Google SEM!A2:L367', 'Data | Non-Brand SEM!A2:H367','Data | Yahoo Native!A2:J48','Data | YouTube!A2:I366']
destination=['rtf.Bing_SEM_Data', 'rtf.Brand_SEM_Data','rtf.BVOS_Data','rtf.GDN_Data','rtf.Google_SEM_Data','rtf.Non_Brand_SEM_Data','rtf.Yahoo_Data','rtf.Youtube_Data'] #scope table example is Global_Goals.Consolidate.
source_destination = dict(zip(source, destination))


taskIDs = ['refresh_Bing_SEM','refresh_Brand_SEM', 'refresh_BVOS', 'refresh_GDN','refresh_Google_SEM', 'refresh_Non_Brand_SEM', 'refresh_Yahoo', 'refresh_Youtube']


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
    df = readSheetData(sheet_service, sheet_id, kwargs['source'])

    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns=lambda x: re.sub('[^a-zA-Z0-9]', '_', x), inplace=True)
    df.rename(columns=lambda x: re.sub('__*', '_', x), inplace=True)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('-', '')
    df.columns = df.columns.str.replace('.', '')
    df.columns = df.columns.str.replace('/', '_')
    df.columns = df.columns.str.replace('__', '_')

    if kwargs['source'] == 'Data | Google SEM!A2:L367' :
        df['Impressions']=df['Impressions'].str.replace(',','')
        df['Impressions']=df['Impressions'].astype(float)
        df['Clicks']=df['Clicks'].str.replace(',','')
        df['Clicks']=df['Clicks'].astype(float)
        df['Date']=df['Date'].astype('datetime64[ns]')
        df['Spend']=df['Spend'].str.replace('$','')
        df['Spend']=df['Spend'].str.replace(',','')
        df['Spend']=df['Spend'].astype(float)
    else:
        df['Impressions']=df['Impressions'].str.replace(',','')
        df['Impressions']=df['Impressions'].astype(int)
        df['Clicks']=df['Clicks'].str.replace(',','')
        df['Clicks']=df['Clicks'].astype(int)
        df['Spend']=df['Spend'].str.replace('$','')
        df['Spend']=df['Spend'].str.replace(',','')
        df['Spend']=df['Spend'].astype(float)
        df['Date']=df['Date'].astype('datetime64[ns]')


    if 'Total_Possible_Imp' in df.columns:
        df['Total_Possible_Imp']=df['Total_Possible_Imp'].str.replace(',','')
        df['Total_Possible_Imp']=df['Total_Possible_Imp'].astype(int)

    if 'Fi_Confirmation_DDA' in df.columns:
        df['Fi_Confirmation_DDA']=df['Fi_Confirmation_DDA'].str.replace(',','')
        df['Fi_Confirmation_DDA']=df['Fi_Confirmation_DDA'].astype(int)

    if 'Email' in df.columns:
        df['Email']=df['Email'].str.replace(',','')
        df['Email']=df['Email'].astype(int)

    if 'Sign_Ups' in df.columns:
        df['Sign_Ups']=df['Sign_Ups'].str.replace(',','')
        df['Sign_Ups']=df['Sign_Ups'].astype(int)

    if 'Conversions' in df.columns:
        df['Conversions']=df['Conversions'].str.replace(',','')
        df['Conversions']=df['Conversions'].astype(int)

    if 'Log_In' in df.columns:
        df['Log_In']=df['Log_In'].str.replace(',','')
        df['Log_In']=df['Log_In'].astype(int)

    if 'Subscriptions' in df.columns:
        df['Subscriptions']=df['Subscriptions'].str.replace(',','')
        df['Subscriptions']=df['Subscriptions'].astype(int)

    if 'Subscription' in df.columns:
        df['Subscription']=df['Subscription'].str.replace(',','')
        df['Subscription']=df['Subscription'].astype(int)

    if 'Sign_In' in df.columns:
        df['Sign_In']=df['Sign_In'].str.replace(',','')
        df['Sign_In']=df['Sign_In'].astype(int)




    df.to_gbq(destination_table= kwargs['destination'], project_id= PROJECT_ID,if_exists='replace') #fail/replace/append




# Define the DAG
dag = DAG('RTF_GFi_Refresh_PROD', description='Refreshes Google Fi Data From gSheets',
          default_args=def_args,
          schedule_interval= "@weekly",
          start_date=datetime(2019,8,6), catchup=False)



start_dummy_task = DummyOperator(task_id='start_dummy_task', dag=dag)
end_dummy_task = DummyOperator(task_id='end_dummy_task', dag=dag)

for source, destination in source_destination.items():
    task = PythonOperator(
    task_id = destination + '_refresh',
    python_callable = refreshGeneric,
    op_kwargs = {
        'source':source,
        'destination':destination},
    dag = dag, provide_context = True)

    start_dummy_task >> task >> end_dummy_task
