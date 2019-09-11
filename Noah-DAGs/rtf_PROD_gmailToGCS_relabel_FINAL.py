from datetime import datetime
from airflow import DAG
import json
from google.oauth2.credentials import Credentials
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import models
import base64
import logging
import os
from essence.analytics.platform import securedcredentials as secure_creds
from googleapiclient.discovery import build
from google.cloud import storage
from apiclient import errors



SCOPES = ['https://mail.google.com/'] # grants all permissions. proceed with caution.

API_NAME = 'gmail'
API_VERSION = 'v1'
userId='rtf_data@essenceglobal.com'


def query_for_message_ids(service, search_query):
    result = service.users().messages().list(userId='me', q=search_query).execute()
    results = result.get('messages')
    if results:
        msg_ids = [r['id'] for r in results]
    else:
        msg_ids = []

    return msg_ids

def ModifyMessage(gmail_service, userId, msg_id, msg_labels):

    message = gmail_service.users().messages().modify(userId=userId, id=msg_id, body=msg_labels).execute()

    label_ids = message['labelIds']
    print(label_ids)

    print('Message ID: %s - With Label IDs %s' % (msg_id, label_ids))


def CreateMsgLabels():
  """Create object to update labels.

  Returns:
    A label update object.
  """
  return {'removeLabelIds': [], 'addLabelIds': ['UNREAD', 'INBOX', 'Label_2']}


def GetAttachments(service, user_id, msg_id, prefix=""):
    message = service.users().messages().get(userId=user_id, id=msg_id).execute()
    for part in message['payload']['parts']:
        if part['filename']:
            if 'data' in part['body']:
                data=part['body']['data']
            else:
                att_id=part['body']['attachmentId']
                att=service.users().messages().attachments().get(userId=user_id, messageId=msg_id,id=att_id).execute()
                data=att['data']
            file_data = base64.urlsafe_b64decode(data.encode('utf-8'))
            path = prefix+part['filename']

            with open(path, 'wb') as f:
                    f.write(file_data)
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('rtf_staging')
            blob = bucket.blob(path)
            blob.upload_from_filename(path)

def processEmailScanning():
  data_key='rtf_data@essenceglobal.com'
  data_value=secure_creds.getDataFromEssenceVault(data_key)
  credentials_dict = json.loads(data_value)
  credentials = Credentials(**credentials_dict)
  gmail_service=build(API_NAME, API_VERSION, credentials=credentials,cache_discovery=False)
  messageID = query_for_message_ids(gmail_service,'has:attachment')
  for i in range(len(messageID)):
      GetAttachments(gmail_service, 'me', messageID[i], prefix="rtf_")
      message = gmail_service.users().messages().modify(userId=userId, id=messageID[i], body={'removeLabelIds': [], 'addLabelIds': ['Label_7576572066746469364']}).execute()

dag = DAG('rtf_PROD_gmailToGCS_relabel_FINAL', description='DAG to fetch gmail attachments and label as Processed',
          schedule_interval=None,
          start_date=datetime(2019, 8, 24), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=1, dag=dag)

py_operator = PythonOperator(task_id='fetchDataFromGmail',
    python_callable=processEmailScanning, dag=dag)

dummy_operator >> py_operator
