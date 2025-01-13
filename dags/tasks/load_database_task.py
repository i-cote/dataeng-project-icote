import pandas as pd
import sqlalchemy

# SQL connection string
DATABASE_URL = "postgresql://appuser:apppassword@app-db:5432/appdb"

# Define file paths
ethnicity_file_path = "/tmp/nyc_detailed_race_and_ethnicity_data_2020.json"
arrest_file_path = "/tmp/nyc_arrest_data_with_geoid.json"

def load_database(**kwargs):
    # Connect to the database
    engine = sqlalchemy.create_engine(DATABASE_URL)
    
    ethnicity_data = pd.read_json(ethnicity_file_path)
    ethnicity_data.to_sql("ethnicity_data", engine, if_exists="replace", index=False)

    arrest_data = pd.read_json(arrest_file_path)

    arrest_data['arrest_date'] = pd.to_datetime(arrest_data['arrest_date']).dt.date

    time_dim_table_name = "time_dimension"

    def build_time_dimension_row(date_val):
        """
        Given a single datetime.date object, return a dictionary
        with the desired columns for the time dimension.
        """
        year = date_val.year
        month = date_val.month
        day = date_val.day
        quarter = (month - 1) // 3 + 1
        day_of_week = date_val.weekday()  # Monday=0, Sunday=6
        week_of_year = date_val.isocalendar()[1]
        
        time_key = year * 10000 + month * 100 + day
        
        return {
            "time_key": time_key,
            "date": date_val,
            "day": day,
            "month": month,
            "quarter": quarter,
            "year": year,
            "day_of_week": day_of_week,      # Monday=0, Sunday=6
            "week_of_year": week_of_year,
            "is_weekend": day_of_week >= 5   # Saturday=5, Sunday=6
        }

    try:
        existing_time_dim = pd.read_sql_table(time_dim_table_name, con=engine)
    except ValueError:
        existing_time_dim = pd.DataFrame(columns=[
            "time_key",
            "date",
            "day",
            "month",
            "quarter",
            "year",
            "day_of_week",
            "week_of_year",
            "is_weekend"
        ])

    known_dates = set(pd.to_datetime(existing_time_dim['date']).dt.date)

    unique_arrest_dates = set(arrest_data['arrest_date'].unique())
    missing_dates = unique_arrest_dates - known_dates

    new_rows = [build_time_dimension_row(d) for d in missing_dates]
    if new_rows:
        new_time_dim_df = pd.DataFrame(new_rows)
        new_time_dim_df.to_sql(time_dim_table_name, engine, if_exists='append', index=False)

    time_dim = pd.read_sql_table(time_dim_table_name, con=engine)

    date_to_time_key = dict(zip(pd.to_datetime(time_dim['date']).dt.date, time_dim['time_key']))

    arrest_data['time_key'] = arrest_data['arrest_date'].apply(lambda d: date_to_time_key[d])

    arrest_data.to_sql("arrests", engine, if_exists="replace", index=False)