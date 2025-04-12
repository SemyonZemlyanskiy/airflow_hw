import datetime as dt
import os
import sys
import warnings

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.join(os.path.dirname(__file__), '..')
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
warnings.filterwarnings("ignore")

from modules.pipeline import pipeline as pipeline
from modules.predict import predict as predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2025, 4, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction8',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:

    pipeline_task = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )
    predict_task = PythonOperator(
        task_id='predict',
        python_callable=predict,
    )
    pipeline_task >> predict_task

