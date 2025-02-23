##############################################################################
# Import necessary modules
##############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils import encode_data, input_features_check, prediction_col_check

###############################################################################
# Define default arguments and DAG
##############################################################################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 7, 30),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

inference_dag = DAG(
    dag_id='Lead_scoring_inference_pipeline',
    default_args=default_args,
    description='Inference pipeline for Lead Scoring System',
    schedule_interval='@daily',
    catchup=False
)

###############################################################################
# Create a task for encode_data() with task_id 'encode_inference_data'
##############################################################################
encode_task = PythonOperator(
    task_id='encode_inference_data',
    python_callable=encode_data,
    dag=inference_dag
)

###############################################################################
# Create a task for input_features_check() with task_id 'check_inference_features'
##############################################################################
features_check_task = PythonOperator(
    task_id='check_inference_features',
    python_callable=input_features_check,
    dag=inference_dag
)

###############################################################################
# Create a task for prediction_col_check() with task_id 'get_model_predictions'
##############################################################################
prediction_task = PythonOperator(
    task_id='get_model_predictions',
    python_callable=prediction_col_check,
    dag=inference_dag
)

###############################################################################
# Define relations between tasks
##############################################################################
encode_task >> features_check_task >> prediction_task
