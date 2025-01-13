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
    
    geography_of_ethnicity = pd.read_json(ethnicity_file_path)
    geography_of_ethnicity.to_sql("geography_of_ethnicity", engine, if_exists="replace", index=False)

    arrest_data = pd.read_json(arrest_file_path)

    arrest_data['arrest_date'] = pd.to_datetime(arrest_data['arrest_date']).dt.date
    
    #Removing columns we are not interested in
    arrest_data = arrest_data.drop(columns=['jurisdiction_code'])
    arrest_data = arrest_data.drop(columns=['arrest_precinct'])
    arrest_data = arrest_data.drop(columns=['arrest_boro'])
    arrest_data = arrest_data.drop(columns=['law_cat_cd'])
    arrest_data = arrest_data.drop(columns=['pd_cd'])
    arrest_data = arrest_data.drop(columns=['ky_cd'])

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

    # Drop the arrest_date column
    arrest_data = arrest_data.drop(columns=['arrest_date'])

    perp_dim_table_name = "perpetrator_dimension"
    
    try:
        existing_perp_dim = pd.read_sql_table(perp_dim_table_name, con=engine)
    except ValueError:
        existing_perp_dim = pd.DataFrame(
            columns=["perpetrator_key", "perp_sex", "perp_race", "age_group"]
        )

    existing_combos = set(
        zip(
            existing_perp_dim["perp_sex"],
            existing_perp_dim["perp_race"],
            existing_perp_dim["age_group"],
        )
    )

    arrest_data["age_group"] = arrest_data["age_group"].astype(str)  # ensure consistent type
    new_combos = set(
        zip(
            arrest_data["perp_sex"],
            arrest_data["perp_race"],
            arrest_data["age_group"],
        )
    )

    missing_combos = new_combos - existing_combos

    if not existing_perp_dim.empty:
        max_existing_key = existing_perp_dim["perpetrator_key"].max()
    else:
        max_existing_key = 0  # table is empty, start from 0

    new_rows = []
    next_key = max_existing_key + 1
    
    for sex, race, age in missing_combos:
        new_rows.append({
            "perpetrator_key": next_key,
            "perp_sex": sex,
            "perp_race": race,
            "age_group": age
        })
        next_key += 1

    if new_rows:
        new_perp_dim_df = pd.DataFrame(new_rows)
        new_perp_dim_df.to_sql(perp_dim_table_name, engine, if_exists="append", index=False)

    perp_dim = pd.read_sql_table(perp_dim_table_name, con=engine)

    combo_to_key = dict(
        zip(
            zip(perp_dim["perp_sex"], perp_dim["perp_race"], perp_dim["age_group"]),
            perp_dim["perpetrator_key"],
        )
    )

    arrest_data["perpetrator_key"] = [
        combo_to_key[(row.perp_sex, row.perp_race, str(row.age_group))]
        for _, row in arrest_data.iterrows()
    ]

    arrest_data = arrest_data.drop(columns=['age_group'])
    arrest_data = arrest_data.drop(columns=['perp_sex'])
    arrest_data = arrest_data.drop(columns=['perp_race'])

    offense_dim_table_name = "offense_dimension"

    try:
        existing_offense_dim = pd.read_sql_table(offense_dim_table_name, con=engine)
    except ValueError:
        existing_offense_dim = pd.DataFrame(
            columns=["offense_key", "pd_desc", "ofns_desc", "law_code"]
        )

    existing_combos = set(
        zip(
            existing_offense_dim["pd_desc"], 
            existing_offense_dim["ofns_desc"], 
            existing_offense_dim["law_code"]
        )
    )

    new_combos = set(
        zip(
            arrest_data["pd_desc"], 
            arrest_data["ofns_desc"], 
            arrest_data["law_code"]
        )
    )

    missing_combos = new_combos - existing_combos

    if not existing_offense_dim.empty:
        max_existing_key = existing_offense_dim["offense_key"].max()
    else:
        max_existing_key = 0  # table is empty, start from 0

    new_rows = []
    next_key = max_existing_key + 1

    for pd_desc, ofns_desc, law_code in missing_combos:
        new_rows.append({
            "offense_key": next_key,
            "pd_desc": pd_desc,
            "ofns_desc": ofns_desc,
            "law_code": law_code
        })
        next_key += 1

    if new_rows:
        new_offense_dim_df = pd.DataFrame(new_rows)
        new_offense_dim_df.to_sql(offense_dim_table_name, engine, if_exists="append", index=False)

    offense_dim = pd.read_sql_table(offense_dim_table_name, con=engine)

    combo_to_key = dict(
        zip(
            zip(offense_dim["pd_desc"], offense_dim["ofns_desc"], offense_dim["law_code"]),
            offense_dim["offense_key"],
        )
    )

    arrest_data["offense_key"] = [
        combo_to_key[(row.pd_desc, row.ofns_desc, row.law_code)]
        for _, row in arrest_data.iterrows()
    ]


    arrest_data = arrest_data.drop(columns=['pd_desc'])
    arrest_data = arrest_data.drop(columns=['ofns_desc'])
    arrest_data = arrest_data.drop(columns=['law_code'])

    arrest_data.to_sql("arrests", engine, if_exists="replace", index=False)