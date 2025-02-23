"""
utils.py - Inference Pipeline Utilities

This module provides functions for the inference pipeline:
  1. encode_data: Reads cleaned inference data from the cleaning DB, encodes features, and writes to a temporary CSV.
  2. input_features_check: Checks that the encoded data has all required features.
  3. load_model: Loads the trained model from disk.
  4. prediction_col_check: Generates predictions using the model and writes them to the DB.
"""

import os
import sqlite3
import pandas as pd
import pickle
from constants import DB_NAME, LEADSCORING_INFERENCE_CSV, MODEL_PATH, PREDICTIONS_OUTPUT

def encode_data():
    """
    Read cleaned inference data from the cleaning DB.
    Since inference data does not have 'app_complete_flag', drop it if present.
    One-hot encode categorical features and save the encoded data to /tmp/inference_encoded.csv.
    """
    conn = sqlite3.connect(DB_NAME)
    # Assume cleaned inference data is in table 'loaded_data_test_case' in the cleaning DB.
    df = pd.read_sql_query("SELECT * FROM loaded_data_test_case", conn)
    conn.close()
    print("Raw inference data loaded with shape:", df.shape)
    
    if 'app_complete_flag' in df.columns:
        df = df.drop(columns=['app_complete_flag'])
    
    # One-hot encode categorical features.
    categorical_cols = ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']
    df_encoded = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
    
    temp_path = '/tmp/inference_encoded.csv'
    df_encoded.to_csv(temp_path, index=False)
    print("Encoded inference data saved to", temp_path)

def input_features_check():
    """
    Check that the encoded inference data has required features.
    Expected features (from training model input) include:
      'total_leads_droppped', 'city_tier', 'referred_lead', and the one-hot encoded columns.
    """
    temp_path = '/tmp/inference_encoded.csv'
    df = pd.read_csv(temp_path)
    expected = ['total_leads_droppped', 'city_tier', 'referred_lead']
    missing = [col for col in expected if col not in df.columns]
    if missing:
        print("Missing expected features:", missing)
    else:
        print("All expected features are present in the encoded data.")

def load_model():
    """
    Load the trained model from disk.
    """
    try:
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        print("Trained model loaded from", MODEL_PATH)
        return model
    except Exception as e:
        print("Error loading model:", e)
        return None

def prediction_col_check():
    """
    Generate predictions using the trained model on the encoded inference data.
    Perform a simple sanity check on the predictions and write them to the DB in table 'inference_predictions'.
    """
    temp_path = '/tmp/inference_encoded.csv'
    df = pd.read_csv(temp_path)
    model = load_model()
    if model is None:
        print("Model not loaded; cannot generate predictions.")
        return
    preds = model.predict(df)
    df_preds = pd.DataFrame({'prediction': preds})
    print("Unique predictions:", df_preds['prediction'].unique())
    
    conn = sqlite3.connect(DB_NAME)
    df_preds.to_sql('inference_predictions', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("Predictions saved to table 'inference_predictions'.")
