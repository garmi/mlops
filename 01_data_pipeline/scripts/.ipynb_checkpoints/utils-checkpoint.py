"""
utils.py - Data Pipeline Utilities

This module implements the data cleaning pipeline in four steps:
  1. load_data_into_db: Load the CSV into a table (loaded_data_test_case) using the full raw schema (45 columns).
  2. map_city_tier: Map the 'city_mapped' column using CITY_TIER_MAPPING and then rename it to 'city_tier'.
  3. map_categorical_vars: Process categorical columns on the mapped table.
  4. interactions_mapping: Produce the final table with the model input schema (7 columns), as expected by unit tests.
"""

import os
import sqlite3
import pandas as pd

from constants import DB_NAME, LEADSCORING_TEST_CSV, UNIT_TEST_DB_FILE_NAME
from significant_categorical_level import SIGNIFICANT_PLATFORM, SIGNIFICANT_UTM_MEDIUM, SIGNIFICANT_UTM_SOURCE
from city_tier_mapping import CITY_TIER_MAPPING
from schema import raw_data_schema, model_input_schema

# For missing numeric columns, use 0; for others, use an empty string.
numeric_defaults = {"total_leads_droppped", "referred_lead", "app_complete_flag"}

# Create a mapped schema by copying raw_data_schema and replacing "city_mapped" with "city_tier"
mapped_data_schema = raw_data_schema.copy()
if "city_mapped" in mapped_data_schema:
    mapped_data_schema[mapped_data_schema.index("city_mapped")] = "city_tier"

def build_dbs():
    """
    Initialize the database by removing any existing DB file and dropping tables.
    """
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
    """
    Load data from the CSV into the 'loaded_data_test_case' table using the full raw schema.
    The CSV has 13 columns; rename the CSV column 'city_tier' to 'city_mapped' (to match raw_data_schema)
    and add missing columns with default values so that the table has 45 columns.
    """
    try:
        df = pd.read_csv(LEADSCORING_TEST_CSV)
        print(f"CSV file '{LEADSCORING_TEST_CSV}' successfully read.")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Rename CSV column "city_tier" to "city_mapped" if necessary.
    if 'city_tier' in df.columns and 'city_mapped' not in df.columns:
        df = df.rename(columns={'city_tier': 'city_mapped'})
    
    # Add missing columns from raw_data_schema with defaults.
    for col in raw_data_schema:
        if col not in df.columns:
            if col in numeric_defaults:
                df[col] = 0
            else:
                df[col] = ""
    
    # Reorder DataFrame to match raw_data_schema (45 columns)
    df = df[raw_data_schema]
    
    conn = sqlite3.connect(DB_NAME)
    df.to_sql('loaded_data_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("Data loaded into 'loaded_data_test_case'.")

def map_city_tier():
    """
    Map city tiers for the loaded data.
    Reads from 'loaded_data_test_case', applies CITY_TIER_MAPPING to the 'city_mapped' column
    (defaulting to 3 if not found), renames it to 'city_tier', and writes the output to
    'city_tier_mapped_test_case' using the mapped_data_schema.
    """
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM loaded_data_test_case", conn)
    
    print("Columns in loaded_data_test_case:", df.columns.tolist())
    
    if 'city_mapped' not in df.columns:
        print("Column 'city_mapped' not found in data.")
        conn.close()
        return

    print("Unique values in 'city_mapped' before mapping:", df['city_mapped'].unique())
    
    # Map city values using CITY_TIER_MAPPING; default to 3 if not found.
    df['city_mapped'] = df['city_mapped'].map(CITY_TIER_MAPPING).fillna(3)
    
    print("Unique values in 'city_mapped' after mapping:", df['city_mapped'].unique())
    
    # Rename 'city_mapped' to 'city_tier'
    df = df.rename(columns={'city_mapped': 'city_tier'})
    
    # Reorder DataFrame to match mapped_data_schema (45 columns)
    try:
        df = df[mapped_data_schema]
    except KeyError as e:
        print("Error subsetting DataFrame with mapped_data_schema:", e)
        conn.close()
        return

    df.to_sql('city_tier_mapped_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("City tier mapping applied and saved to 'city_tier_mapped_test_case'.")

def map_categorical_vars():
    """
    Process categorical variables by converting values not in the significant lists to 'others'.
    Reads from 'city_tier_mapped_test_case', updates the categorical columns,
    reorders to match mapped_data_schema, and writes to 'categorical_variables_mapped_test_case'.
    """
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM city_tier_mapped_test_case", conn)
    
    for col in ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']:
        if col not in df.columns:
            print(f"Column '{col}' not found in data.")
            conn.close()
            return

    df['first_platform_c'] = df['first_platform_c'].apply(lambda x: x if x in SIGNIFICANT_PLATFORM else 'others')
    df['first_utm_medium_c'] = df['first_utm_medium_c'].apply(lambda x: x if x in SIGNIFICANT_UTM_MEDIUM else 'others')
    df['first_utm_source_c'] = df['first_utm_source_c'].apply(lambda x: x if x in SIGNIFICANT_UTM_SOURCE else 'others')
    
    # Reorder DataFrame to match mapped_data_schema.
    df = df[mapped_data_schema]
    
    df.to_sql('categorical_variables_mapped_test_case', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print("Categorical variables mapped and saved to 'categorical_variables_mapped_test_case'.")

def interactions_mapping():
    """
    Produce the final interactions table.
    Reads from 'categorical_variables_mapped_test_case', optionally creates interaction features,
    renames the column 'city_mapped' back to 'city_tier' (if necessary) so that the final header matches expected output,
    and subsets the DataFrame to the model input schema (7 columns).
    Writes the final DataFrame to 'interactions_mapped_test_case'.
    """
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT * FROM categorical_variables_mapped_test_case", conn)
    
    # (Optional) Create an interaction feature; for example:
    if 'first_platform_c' in df.columns and 'first_utm_medium_c' in df.columns:
        df['platform_utm_interaction'] = df['first_platform_c'] + '_' + df['first_utm_medium_c']
    else:
        print("Required columns for interaction mapping not found.")
        conn.close()
        return
    
    # Rename 'city_mapped' back to 'city_tier' if present (it might already be 'city_tier').
    if 'city_mapped' in df.columns:
        df = df.rename(columns={'city_mapped': 'city_tier'})
    
    # Subset DataFrame to match the model input schema (7 columns) as defined in friend's schema.
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
