import requests

import shutil

def fetch_pop_facts_xlsx_file(is_offline):

    if is_offline:
        shutil.copy('/offline_data/nyc_detailed_race_and_ethnicity_data_2020.xlsx','/tmp/nyc_detailed_race_and_ethnicity_data_2020.xlsx')
        return

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