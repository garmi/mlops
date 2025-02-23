##############################################################################
# Import necessary modules
##############################################################################
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd

# Import our inference pipeline functions from utils.py
from utils import load_inference_data, preprocess_inference_data, load_model, predict, save_predictions

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
    schedule_interval='@daily',  # adjust schedule as needed
    catchup=False
)

###############################################################################
# Create a task for data preprocessing with task_id 'preprocess_inference_data'
##############################################################################
def preprocess_inference(**kwargs):
    """
    Load cleaned data from the database, preprocess the inference features,
    and save the preprocessed data to a temporary CSV file.
    """
    df = load_inference_data()
    X = preprocess_inference_data(df)
    preprocessed_path = '/tmp/inference_features.csv'
    X.to_csv(preprocessed_path, index=False)
    print("Preprocessed inference data saved to", preprocessed_path)

preprocess_task = PythonOperator(
    task_id='preprocess_inference_data',
    python_callable=preprocess_inference,
    dag=inference_dag
)

###############################################################################
# Create a task for model inference with task_id 'get_predictions'
##############################################################################
def get_predictions(**kwargs):
    """
    Load the preprocessed data, load the trained model, make predictions,
    and save the predictions to disk.
    """
    preprocessed_path = '/tmp/inference_features.csv'
    X = pd.read_csv(preprocessed_path)
    model = load_model()
    preds = predict(model, X)
    save_predictions(preds)

prediction_task = PythonOperator(
    task_id='get_predictions',
    python_callable=get_predictions,
    dag=inference_dag
)

###############################################################################
# Define relations between tasks
##############################################################################
preprocess_task >> prediction_task
