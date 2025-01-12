import json
import pandas as pd

def convert_xlsx_to_json(**kwargs):

    input_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.xlsx'  # Path to the input Excel file

    df = pd.read_excel(input_file, header=[0, 1, 2, 3])
    df.columns = ['_'.join(col).strip() for col in df.columns.values]
    # Convert to dictionary first
    data_dict = df.to_dict(orient='records')

    # Filter out entries with population below 94 000
    filtered_data = [
        entry for entry in data_dict
        if entry.get('2020 Detailed Race and Ethnicity Groups, Alone or In Any Combination_Total Population_Number_Pop', 0) >= 800000
    ]
    # Convert to JSON string with custom formatting
    json_string = json.dumps(filtered_data, indent=2)
    # Save to file
    with open('/opt/airflow/config/pop.json', 'w') as f:
        f.write(json_string)