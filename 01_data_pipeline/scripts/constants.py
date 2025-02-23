import os

# Base directory: assumes this file is in your scripts folder.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path where the cleaning pipeline database will be stored.
DB_NAME = os.path.join(BASE_DIR, "utils_output.db")

# Path to the CSV file for data cleaning.
LEADSCORING_TEST_CSV = os.path.join(BASE_DIR, "leadscoring_test.csv")

# (Optional) Unit test database file.
UNIT_TEST_DB_FILE_NAME = os.path.join(BASE_DIR, "unit_test_cases.db")
