"""
utils.py - Inference Pipeline Utilities

This module provides functions for loading cleaned data from the cleaning pipeline’s database,
preprocessing the inference features, loading the trained model, making predictions, and saving them.
"""

import os
import sqlite3
import pandas as pd
import pickle
from constants import DB_NAME, MODEL_PATH, PREDICTIONS_OUTPUT

def load_inference_data():
    """
    Load inference data from the cleaning database.
    Assumes the cleaning pipeline writes the final data to the table 'interactions_mapped_test_case'.
    """
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM interactions_mapped_test_case", conn)
    conn.close()
    print("Inference data loaded with shape:", df.shape)
    return df

def preprocess_inference_data(df):
    """
    Preprocess the inference data.
    Assumes the model input features are:
      'total_leads_droppped', 'city_tier', 'referred_lead', 
      'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c', 
      'app_complete_flag'
    For inference, we drop the target column ('app_complete_flag') from features
    and perform one-hot encoding on the categorical features.
    """
    # Define features; note: for inference we typically do not have the target
    features = ['total_leads_droppped', 'city_tier', 'referred_lead', 
                'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']
    
    X = df[features]
    
    # One-hot encode categorical features
    X = pd.get_dummies(X, columns=['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c'], drop_first=True)
    
    print("Inference data preprocessed. Feature matrix shape:", X.shape)
    return X

def load_model():
    """
    Load the trained model from disk.
    """
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    print("Trained model loaded from", MODEL_PATH)
    return model

def predict(model, X):
    """
    Make predictions using the trained model.
    """
    predictions = model.predict(X)
    print("Predictions made. Number of predictions:", len(predictions))
    return predictions

def save_predictions(predictions):
    """
    Save predictions to a CSV file.
    """
    df = pd.DataFrame({'prediction': predictions})
    df.to_csv(PREDICTIONS_OUTPUT, index=False)
    print("Predictions saved to", PREDICTIONS_OUTPUT)
