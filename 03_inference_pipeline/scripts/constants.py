import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the cleaning pipeline database (for inference, we use the cleaned inference DB)
DB_NAME = os.path.join(BASE_DIR, "lead_scoring_data_cleaning.db")

# Path to the inference CSV (raw inference data; note: does not contain 'app_complete_flag')
LEADSCORING_INFERENCE_CSV = os.path.join(BASE_DIR, "leadscoring_inference.csv")

# Path to the trained model file.
MODEL_PATH = os.path.join(BASE_DIR, "trained_model.pkl")

# Path to save predictions.
PREDICTIONS_OUTPUT = os.path.join(BASE_DIR, "predictions.csv")
