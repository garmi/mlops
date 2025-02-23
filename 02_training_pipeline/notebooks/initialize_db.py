import sqlite3

db_path = "/home/Assignment/02_training_pipeline/notebooks/lead_scoring_model_experimentation.db"

# Create a new SQLite database (if not exists)
conn = sqlite3.connect(db_path)
conn.execute("VACUUM")  # Optimizes the database
conn.close()

print("✅ MLflow database initialized!")
