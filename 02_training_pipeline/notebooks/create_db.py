import pandas as pd
import sqlite3

# Define paths
csv_path = "/home/Assignment/02_training_pipeline/notebooks/Data/cleaned_data.csv"
db_path = "/home/Assignment/01_data_pipeline/scripts/lead_scoring_data_cleaning.db"
table_name = "cleaned_leads"

# Load CSV
df = pd.read_csv(csv_path)
print(f"Loaded {len(df)} rows from CSV.")

# Save to SQLite
conn = sqlite3.connect(db_path)
df.to_sql(table_name, conn, if_exists="replace", index=False)
conn.close()

print(f"✅ Data successfully written to {db_path} in table {table_name}")
