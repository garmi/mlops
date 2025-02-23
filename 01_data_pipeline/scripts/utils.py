"""
utils.py - Data Pipeline Utilities

This module implements the data cleaning pipeline in five steps:
  1. build_dbs(): Initialize the database.
  2. load_data_into_db(): Load the CSV into the DB using the full raw schema (45 columns).
  3. map_city_tier(): Map the 'city_mapped' column using CITY_TIER_MAPPING, then rename it to 'city_tier'.
  4. map_categorical_vars(): Convert insignificant categorical values to "others".
  5. interactions_mapping(): Map interaction columns to create a final table.
"""

import os
import sqlite3
import pandas as pd
from constants import DB_NAME, LEADSCORING_TEST_CSV, UNIT_TEST_DB_FILE_NAME
from city_tier_mapping import CITY_TIER_MAPPING
from significant_categorical_level import SIGNIFICANT_PLATFORM, SIGNIFICANT_UTM_MEDIUM, SIGNIFICANT_UTM_SOURCE
from schema import raw_data_schema, model_input_schema

# For missing numeric columns, use 0; for others, use an empty string.
numeric_defaults = {"total_leads_droppped", "referred_lead", "app_complete_flag"}

def build_dbs():
    if os.path.isfile(DB_NAME):
        os.remove(DB_NAME)
        print(f"Existing database '{DB_NAME}' removed.")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS loaded_data_test_case;")
    cursor.execute("DROP TABLE IF EXISTS city_tier_mapped_test_case;")
    cursor.execute("DROP TABLE IF EXISTS categorical_variables_mapped_test_case;")
    cursor.execute("DROP TABLE IF EXISTS interactions_mapped_test_case;")
    conn.commit()
    conn.close()
    print("Database initialized.")

def load_data_into_db():
    try:
        df = pd.read_csv(LEADSCORING_TEST_CSV)
        print(f"CSV file '{LEADSCORING_TEST_CSV}' successfully read.")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # The CSV contains 13 columns; rename "city_tier" to "city_mapped"
    if 'city_tier' in df.columns and 'city_mapped' not in df.columns:
        df = df.rename(columns={'city_tier': 'city_mapped'})
    
    # Add missing columns from raw_data_schema with default values.
    for col in raw_data_schema:
        if col not in df.columns:
            df[col] = 0 if col in numeric_defaults else ""
    
    # Reorder to match raw_data_schema (45 columns)
    df = df[raw_data_schema]
    
    conn = sqlite3.connect(DB_NAME)
    df.to_sql('loaded_data_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("Data loaded into 'loaded_data_test_case'.")

def map_city_tier():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM loaded_data_test_case", conn)
    print("Columns in loaded_data_test_case:", df.columns.tolist())
    
    if 'city_mapped' not in df.columns:
        print("Column 'city_mapped' not found.")
        conn.close()
        return

    print("Unique values in 'city_mapped' before mapping:", df['city_mapped'].unique())
    df['city_mapped'] = df['city_mapped'].map(CITY_TIER_MAPPING).fillna(3)
    print("Unique values in 'city_mapped' after mapping:", df['city_mapped'].unique())
    df = df.rename(columns={'city_mapped': 'city_tier'})
    
    # Construct mapped schema: replace "city_mapped" with "city_tier" in raw_data_schema.
    mapped_schema = raw_data_schema.copy()
    if "city_mapped" in mapped_schema:
        mapped_schema[mapped_schema.index("city_mapped")] = "city_tier"
    try:
        df = df[mapped_schema]
    except KeyError as e:
        print("Error subsetting DataFrame with mapped_schema:", e)
        conn.close()
        return

    df.to_sql('city_tier_mapped_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("City tier mapping applied and saved to 'city_tier_mapped_test_case'.")

def map_categorical_vars():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM city_tier_mapped_test_case", conn)
    for col in ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']:
        if col not in df.columns:
            print(f"Column '{col}' not found.")
            conn.close()
            return

    df['first_platform_c'] = df['first_platform_c'].apply(lambda x: x if x in SIGNIFICANT_PLATFORM else 'others')
    df['first_utm_medium_c'] = df['first_utm_medium_c'].apply(lambda x: x if x in SIGNIFICANT_UTM_MEDIUM else 'others')
    df['first_utm_source_c'] = df['first_utm_source_c'].apply(lambda x: x if x in SIGNIFICANT_UTM_SOURCE else 'others')
    
    mapped_schema = raw_data_schema.copy()
    if "city_mapped" in mapped_schema:
        mapped_schema[mapped_schema.index("city_mapped")] = "city_tier"
    df = df[mapped_schema]
    
    df.to_sql('categorical_variables_mapped_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("Categorical variables mapped and saved to 'categorical_variables_mapped_test_case'.")

def interactions_mapping():
    """
    For this assignment, the final output for training/inference is expected to use only the model input schema.
    """
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM categorical_variables_mapped_test_case", conn)
    # Optionally create interaction feature
    if 'first_platform_c' in df.columns and 'first_utm_medium_c' in df.columns:
        df['platform_utm_interaction'] = df['first_platform_c'] + '_' + df['first_utm_medium_c']
    else:
        print("Required columns for interaction mapping not found.")
        conn.close()
        return

    # For final output, subset to model_input_schema (7 columns)
    try:
        df = df[model_input_schema]
    except KeyError as e:
        print("Error subsetting DataFrame with model_input_schema:", e)
        conn.close()
        return

    df.to_sql('interactions_mapped_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("Interactions mapping applied and saved to 'interactions_mapped_test_case'.")
