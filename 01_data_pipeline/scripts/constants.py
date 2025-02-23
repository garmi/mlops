import os

# Base directory: assumes this file is in your scripts folder.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Full path to the output SQLite database.
DB_NAME = os.path.join(BASE_DIR, "utils_output.db")

# Full path to the CSV file.
LEADSCORING_TEST_CSV = os.path.join(BASE_DIR, "leadscoring_test.csv")

# Full path to the unit test cases database.
UNIT_TEST_DB_FILE_NAME = os.path.join(BASE_DIR, "unit_test_cases.db")
