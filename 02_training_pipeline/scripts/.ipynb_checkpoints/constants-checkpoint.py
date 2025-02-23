'''
filename: constants.py
creator: shashank.gupta
version: 1
'''
#############################################################################
# Define Constants for the Lead Scoring Project
# #############################################################################

import os

# Database Paths
DB_PATH = "/home/Assignment/01_data_pipeline/scripts/"
DB_FILE_NAME = "lead_scoring_data_cleaning.db"
TRAINING_DB_FILE = "lead_scoring_training.db"
UNIT_TEST_DB_FILE = "unit_test_cases.db"

# Data Directory
DATA_DIRECTORY = "/home/Assignment/01_data_pipeline/scripts/data/"

# MLflow Configuration
MLFLOW_TRACKING_URI = "http://localhost:6006"
MLFLOW_EXPERIMENT_NAME = "Lead Scoring Training"
MLFLOW_MODEL_NAME = "lead_scoring_model"

# Training Configuration
SIGNIFICANT_FEATURES = [
    "total_leads_droppped", "city_tier", "referred_lead", "app_complete_flag",
    "first_platform_c", "first_utm_medium_c", "first_utm_source_c"
]

# Hyperparameter Tuning
LIGHTGBM_PARAMS = {
    "num_leaves": [20, 50, 100, 200],
    "learning_rate": [0.01, 0.05, 0.1, 0.2],
    "n_estimators": [50, 100, 200, 500],
    "max_depth": [3, 5, 7, 10]
}

print("Constants Loaded Successfully")
