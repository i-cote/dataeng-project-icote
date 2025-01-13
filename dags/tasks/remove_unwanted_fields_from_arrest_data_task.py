import json

def remove_unwanted_fields_from_arrest_data():
    input_file = '/tmp/nyc_arrest_data_with_geoid.json'
    output_file = '/tmp/nyc_arrest_data_with_geoid.json'

    # Fields to remove from each entry
    fields_to_remove = [
        "x_coord_cd", "y_coord_cd", "latitude", "longitude", "lon_lat",
        ":@computed_region_efsh_h5xi", ":@computed_region_f5dn_yrer",
        ":@computed_region_yeji_bk3q", ":@computed_region_92fq_4b7q",
        ":@computed_region_sbqj_enih"
    ]

    try:
        # Load data from the input file
        with open(input_file, 'r') as file:
            data = json.load(file)

        # Remove specified fields from each entry
        for entry in data:
            for field in fields_to_remove:
                entry.pop(field, None)

        # Write the updated data to the output file
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=2)

    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from the file {input_file}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")