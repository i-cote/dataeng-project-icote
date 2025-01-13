import json
import requests

# Define the script for fetching JSON data
def fetch_arrest_data_from_api():
    # API endpoint and parameters
    API_ENDPOINT = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json?$limit=1000"
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