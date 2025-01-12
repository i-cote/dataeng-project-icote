import json

def remove_entries_with_wrong_geoType():
    input_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.json'
    output_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.json'

    try:
        # Load data from the input file
        with open(input_file, 'r') as file:
            data = json.load(file)

        # Filter the data to keep only entries with GeoType set to 'CT2020'
        filtered_data = [entry for entry in data if entry.get('Geography_Unnamed: 1_level_1_Unnamed: 1_level_2_GeoType') == 'CT2020']

        # Write the filtered data to the output file
        with open(output_file, 'w') as file:
            json.dump(filtered_data, file, indent=2)

    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from the file {input_file}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")