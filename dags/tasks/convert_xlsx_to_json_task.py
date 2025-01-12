import json
import pandas as pd

def convert_xlsx_to_json(**kwargs):

    input_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.xlsx'
    output_file = '/tmp/nyc_detailed_race_and_ethnicity_data_2020.json'

    df = pd.read_excel(input_file, header=[0, 1, 2, 3])
    df.columns = ['_'.join(col).strip() for col in df.columns.values]

    data_dict = df.to_dict(orient='records')

    json_string = json.dumps(data_dict, indent=2)

    with open(output_file, 'w') as f:
        f.write(json_string)