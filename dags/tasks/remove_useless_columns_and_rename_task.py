import json

def remove_useless_columns_and_rename():
    input_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.json'
    output_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.json'

    fields_to_remove = {"Orig Order", "GeoType", "BCT2020", "GeoName", "CDType", "NTAType"}

    try:
        # Load data from the input file
        with open(input_file, 'r') as file:
            data = json.load(file)

        # Process each entry in the data
        for entry in data:
            new_entry = {}
            for key, value in entry.items():
                if key.startswith('Geography'):
                    new_key = key[48:]  # Remove the first 48 characters
                elif key.startswith('2020'):
                    new_key = 'Ethnicity_' + key[69:]  # Replace first 69 characters with 'Ethnicity_'
                else:
                    new_key = key

                if new_key not in fields_to_remove:
                    new_entry[new_key] = value

            entry.clear()
            entry.update(new_entry)

        # Write the filtered and renamed data to the output file
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=2)

    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from the file {input_file}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")