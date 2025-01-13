import json
import requests

# Define the script for fetching JSON data
def fetch_arrest_data_from_api():
    # API endpoint and parameters
    API_ENDPOINT = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json"
    API_TOKEN = "iAn5FiC7n6AnwsEyG8JNn9fYt"  # Replace with your API token if required

    # Headers for API request (include your token if necessary)
    headers = {
        "X-App-Token": API_TOKEN
    } if API_TOKEN else {}

    offset_step = 600
    limit = 3
    max_calls = 50
    aggregated_data = []

    for i in range(max_calls):
        offset = i * offset_step
        params = {
            "$limit": limit,
            "$offset": offset
        }

        response = requests.get(API_ENDPOINT,headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"Call {i+1}: Fetched records {offset} to {offset + limit - 1}")
            aggregated_data.extend(data)  # Aggregate the data
        else:
            print(f"Error: API call {i+1} failed with status code {response.status_code}")

    # Write aggregated data to a JSON file
    with open('/tmp/nyc_arrest_data.json', 'w') as file:
        json.dump(aggregated_data, file, indent=4)
