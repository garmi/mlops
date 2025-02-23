'''
filename: utils.py
functions: encode_features, get_train_model
creator: shashank.gupta
version: 1
'''
#############################################################################
# Import necessary modules and files
# #############################################################################

import pandas as pd
import os
import sqlite3
from sqlite3 import Error
import sys

sys.path.append('/home/Assignment/')  
sys.path.append('/home/Assignment/airflow/dags/')  

from constants import DB_FILE_NAME, DB_PATH, DATA_DIRECTORY, INTERACTION_MAPPING

from mapping.city_tier_mapping import city_tier_mapping
from mapping.significant_categorical_level import list_platform, list_medium, list_source

###############################################################################
# Define function to initialize the database
# ##############################################################################

def build_dbs():
    try:
        db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
        print("Initializing Database at:", db_full_path)
        
        conn = sqlite3.connect(db_full_path)
        cursor = conn.cursor()
        
        # Create necessary tables if they do not exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS loaded_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_mapped TEXT,
                first_platform_c TEXT,
                first_utm_medium_c TEXT,
                first_utm_source_c TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS city_tier_mapped (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city_mapped TEXT,
                city_tier INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categorical_variables_mapped (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_platform_c TEXT,
                first_utm_medium_c TEXT,
                first_utm_source_c TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Error in database initialization: {e}")

###############################################################################
# Define function to encode categorical features
# ##############################################################################

def encode_features(df):
    df["first_platform_c"] = df["first_platform_c"].apply(lambda x: x if x in list_platform else "others")
    df["first_utm_medium_c"] = df["first_utm_medium_c"].apply(lambda x: x if x in list_medium else "others")
    df["first_utm_source_c"] = df["first_utm_source_c"].apply(lambda x: x if x in list_source else "others")
    return df

###############################################################################
# Define function to train and get model
# ##############################################################################

def get_train_model(df):
    from pycaret.classification import setup, compare_models
    import mlflow
    
    exp1 = setup(data=df, target='app_complete_flag', normalize=False, transformation=False, silent=True, 
                 remove_perfect_collinearity=True, session_id=42)
    best_model = compare_models()
    mlflow.set_tracking_uri("http://localhost:6006")
    mlflow.set_experiment("Lead Scoring Training")
    
    with mlflow.start_run():
        mlflow.pycaret.log_model(best_model, "lead_scoring_model")
    
    return best_model

print("Updated utils.py Loaded Successfully")
