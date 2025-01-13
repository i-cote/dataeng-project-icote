import pandas as pd
import sqlalchemy

# SQL connection string
DATABASE_URL = "postgresql://appuser:apppassword@app-db:5432/appdb"  # Replace with your database connection string

# Define file paths
ethnicity_file_path = "/tmp/nyc_detailed_race_and_ethnicity_data_2020.json"
arrest_file_path = "/tmp/nyc_arrest_data_with_geoid.json"

# Functions
def load_database(**kwargs):
    # Connect to the database
    engine = sqlalchemy.create_engine(DATABASE_URL)
    
    # Load and process ethnicity data
    ethnicity_data = pd.read_json(ethnicity_file_path)
    ethnicity_data.to_sql("ethnicity_data", engine, if_exists="replace", index=False)

    # Load and process arrest data
    arrest_data = pd.read_json(arrest_file_path)
    arrest_data['arrest_date'] = pd.to_datetime(arrest_data['arrest_date']).dt.date
    arrest_data.to_sql("arrests", engine, if_exists="replace", index=False)