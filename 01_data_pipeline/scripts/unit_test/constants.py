import os

# Base directory: assumes this file is in your scripts folder.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Full path to the output SQLite database.
DB_NAME = os.path.join(BASE_DIR, "utils_output.db")

# Full path to the test CSV file. Ensure that this CSV is present in the same folder.
LEADSCORING_TEST_CSV = os.path.join(BASE_DIR, "leadscoring_test.csv")

# Full path to the unit test cases database.
UNIT_TEST_DB_FILE_NAME = os.path.join(BASE_DIR, "unit_test_cases.db")


INTERACTION_MAPPING = "interaction_mapping.csv" # : path to the csv file containing interaction's mappings
INDEX_COLUMNS_TRAINING = ['created_date', 'city_tier', 'first_platform_c',
                'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
                'referred_lead', 'app_complete_flag']

INDEX_COLUMNS_INFERENCE = ['created_date', 'city_tier', 'first_platform_c',
                'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
                'referred_lead']

NOT_FEATURES = ['created_date', 'assistance_interaction', 'career_interaction',
                'payment_interaction', 'social_interaction', 'syllabus_interaction']
