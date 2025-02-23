# utils.py - Training Pipeline Utilities

import os
import sqlite3
import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from constants import DB_NAME, MODEL_PATH

def load_training_data():
    """
    Load training data from the cleaning pipeline database.
    Assumes the final cleaning output is in the table 'interactions_mapped_test_case'.
    """
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM interactions_mapped_test_case", conn)
    conn.close()
    print("Training data loaded with shape:", df.shape)
    return df

def preprocess_data(df):
    """
    Preprocess the training data.
    Assumes the target variable is 'app_complete_flag' and features are:
    'total_leads_droppped', 'city_tier', 'referred_lead', 'first_platform_c',
    'first_utm_medium_c', and 'first_utm_source_c'.
    One-hot encode categorical variables.
    """
    features = ['total_leads_droppped', 'city_tier', 'referred_lead',
                'first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']
    target = 'app_complete_flag'
    X = df[features]
    y = df[target]
    X = pd.get_dummies(X, columns=['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c'], drop_first=True)
    print("Preprocessing completed. Feature matrix shape:", X.shape)
    return X, y

def train_model(X, y):
    """
    Train a logistic regression model and evaluate it.
    """
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print("Test Accuracy:", acc)
    print("Classification Report:")
    print(classification_report(y_test, y_pred))
    return model

def save_model(model):
    """
    Save the trained model to disk.
    """
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump(model, f)
    print("Model saved to", MODEL_PATH)
