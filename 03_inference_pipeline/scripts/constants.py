import os

# Base directory: assumes this file is in your training pipeline folder.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the cleaning pipeline database (utils_output.db)
DB_NAME = os.path.join(BASE_DIR, "utils_output.db")

# Path to the CSV file (if needed; for training it was used, for inference we assume cleaned data is in the DB)
LEADSCORING_TEST_CSV = os.path.join(BASE_DIR, "leadscoring_test.csv")

# Unit test DB file (if used)
UNIT_TEST_DB_FILE_NAME = os.path.join(BASE_DIR, "unit_test_cases.db")

# Path to the trained model (saved from your training pipeline)
MODEL_PATH = os.path.join(BASE_DIR, "trained_model.pkl")

# Where to save predictions
PREDICTIONS_OUTPUT = os.path.join(BASE_DIR, "predictions.csv")
