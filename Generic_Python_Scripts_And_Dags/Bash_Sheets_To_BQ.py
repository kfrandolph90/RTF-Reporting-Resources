from airflow import DAG
from airflow import models
import logging
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator


def build_default_args(**kwargs):
    default_args = {
    'owner': 'Test',
    'email': ['noah.hazan@essenceglobal.com'],
    'queue': 'default',
    'execution_timeout': timedelta(hours=1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    }
    default_args.update(kwargs)
    return default_args




def get_together(**kwargs):
    #Scope, this one is good for everything-spreadsheets (reading and writing), google drive and bigquery.
    sheet = service.spreadsheets()
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
    shellCmd=models.Variable.get('workerShellCmd')
    logging.info(shellCmd)
    shellCommandTask = BashOperator(
        task_id='customShellCommand',
        bash_command="bq query --replace --destination_table=rtf.BVOS_Data 'SELECT * FROM rtf.BVOS_Data_gSheet'" ,
        dag=dag)



dag = DAG('BashCommandOnWorker_Testing', description='It executes shell command on a worker',
          default_args=build_default_args(start_date=datetime(2019, 8, 2), catchup=False),
          schedule_interval=None)


py_operator = PythonOperator(task_id='get_together', python_callable=get_together, dag=dag, provide_context=True)

py_operator
