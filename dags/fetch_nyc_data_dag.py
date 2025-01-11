from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import json

# Define the script for fetching JSON data
def fetch_and_log_nyc_data():
    # API endpoint and parameters
    API_ENDPOINT = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json?$limit=5"
    API_TOKEN = "iAn5FiC7n6AnwsEyG8JNn9fYt"  # Replace with your API token if required

    # Headers for API request (include your token if necessary)
    headers = {
        "X-App-Token": API_TOKEN
    } if API_TOKEN else {}

    try:
        # Fetch the data from the API
        response = requests.get(API_ENDPOINT, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP issues

        data = response.json()

        if not data:
            print("No data available from the API.")
            return

        # Save data for further processing
        with open('/tmp/nyc_arrest_data.json', 'w') as f:
            json.dump(data, f)

        print('Arrest data successfully saved')

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from the API: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Define the script for downloading the CSV file
def fetch_csv_file():
    # Define the URL and output path
    URL = "https://www.nyc.gov/assets/planning/download/office/planning-level/nyc-population/census2020/nyc_detailed-race-and-ethnicity-data_2020_core-geographies.xlsx"

    OUTPUT_PATH = "/tmp/nyc_detailed_race_and_ethnicity_data_2020.xlsx"

    try:
        # Fetch the file using requests
        response = requests.get(URL)
        response.raise_for_status()  # Raise an error for HTTP issues

        # Write the content to a file
        with open(OUTPUT_PATH, 'wb') as file:
            file.write(response.content)

        print(f"File successfully downloaded to {OUTPUT_PATH}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Define the script to derive GEOID for each arrest record
def derive_geoid():
    # Load arrest data
    try:
        with open('/tmp/nyc_arrest_coordinates_validated.json', 'r') as f:
            arrest_data = json.load(f)
    except Exception as e:
        print(f"Error loading arrest data: {e}")
        return

    # Census Geocoder API endpoint
    GEOCODER_API = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

    updated_data = []

    for record in arrest_data:
        lat = record.get("latitude")
        lon = record.get("longitude")

        if not lat or not lon:
            record["geoid"] = None
            updated_data.append(record)
            continue

        params = {
            "x": lon,
            "y": lat,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json"
        }

        try:
            response = requests.get(GEOCODER_API, params=params)
            response.raise_for_status()

            geographies = response.json()
            geoid = geographies.get("result", {}).get("geographies", {}).get("Census Tracts", [{}])[0].get("GEOID")
            record["geoid"] = geoid
        except Exception as e:
            print(f"Error retrieving GEOID for record with lat={lat}, lon={lon}: {e}")
            record["geoid"] = None

        updated_data.append(record)

    # Save updated data with GEOID
    print(json.dumps(updated_data, indent=4))
    try:
        with open('/tmp/nyc_arrest_data_with_geoid.json', 'w') as f:
            json.dump(updated_data, f)
        print("Arrest data updated with GEOIDs and saved.")
    except Exception as e:
        print(f"Error saving updated arrest data: {e}")


# Define the script to validate coordinates within NYC boundaries
def validate_coordinates():
    # NYC bounding box (approximate)
    NYC_BOUNDS = {
        "min_lat": 40.4774,
        "max_lat": 40.9176,
        "min_lon": -74.2591,
        "max_lon": -73.7004
    }

    try:
        # Load arrest data with GEOIDs
        with open('/tmp/nyc_arrest_data.json', 'r') as f:
            arrest_data = json.load(f)

        validated_data = []

        for record in arrest_data:
            lat = record.get("latitude")
            lon = record.get("longitude")

            if lat and lon:
                lat, lon = float(lat), float(lon)
                if (NYC_BOUNDS["min_lat"] <= lat <= NYC_BOUNDS["max_lat"] and
                        NYC_BOUNDS["min_lon"] <= lon <= NYC_BOUNDS["max_lon"]):
                    record["within_nyc"] = True
                else:
                    record["within_nyc"] = False
            else:
                record["within_nyc"] = False

            validated_data.append(record)

        # Save validated data
        with open('/tmp/nyc_arrest_coordinates_validated.json', 'w') as f:
            json.dump(validated_data, f)
        print("Arrest data validated for NYC boundaries and saved.")

    except Exception as e:
        print(f"Error during validation: {e}")


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'fetch_nyc_data_and_csv',
    default_args=default_args,
    description='Fetch NYC data and download additional CSV file',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

# Define the PythonOperators
fetch_data_task = PythonOperator(
    task_id='fetch_and_log_nyc_data',
    python_callable=fetch_and_log_nyc_data,
    dag=dag
)

fetch_csv_task = PythonOperator(
    task_id='fetch_csv_file',
    python_callable=fetch_csv_file,
    dag=dag
)

derive_geoid_task = PythonOperator(
    task_id='derive_geoid',
    python_callable=derive_geoid,
    dag=dag
)

validate_coordinates_task = PythonOperator(
    task_id='validate_coordinates',
    python_callable=validate_coordinates,
    dag=dag
)


# Set task dependencies
fetch_data_task >> validate_coordinates_task >> derive_geoid_task >> fetch_csv_task
