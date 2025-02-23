'''
filename: lead_scoring_training_pipeline.py
creator: shashank.gupta
version: 1
'''
#############################################################################
# Import necessary modules and files
# #############################################################################

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append('/home/Assignment/')  
sys.path.append('/home/Assignment/airflow/dags/')  

from utils import encode_features, get_train_model
from constants import DB_FILE_NAME, DB_PATH

###############################################################################
# Define the DAG
# ##############################################################################

def train_lead_scoring_model():
    import sqlite3
    import pandas as pd
    
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    conn = sqlite3.connect(db_full_path)
    df = pd.read_sql("SELECT * FROM cleaned_leads", conn)
    conn.close()
    
    df = encode_features(df)
    best_model = get_train_model(df)
    print("Model Training Completed Successfully!")

with DAG(
    dag_id='lead_scoring_training_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 22),
    catchup=False,
    tags=['lead_scoring', 'training']
) as dag:

    train_model_task = PythonOperator(
        task_id='train_lead_scoring_model',
        python_callable=train_lead_scoring_model
    )

    train_model_task
