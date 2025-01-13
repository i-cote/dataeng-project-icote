from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from tasks.convert_xlsx_to_json_task import convert_xlsx_to_json
from tasks.fetch_arrest_data_from_api_task import fetch_arrest_data_from_api
from tasks.fetch_pop_facts_xlsx_file_task import fetch_pop_facts_xlsx_file
from tasks.derive_geoid_task import derive_geoid
from tasks.validate_coordinates_task import validate_coordinates
from tasks.remove_entries_with_minTotalPop_task import remove_entries_with_minTotalPop
from tasks.remove_entries_with_wrong_geoType_task import remove_entries_with_wrong_geoType
from tasks.remove_useless_columns_and_rename_task import remove_useless_columns_and_rename
from tasks.create_star_schema_task import create_star_schema
from tasks.load_database_task import load_database
from tasks.remove_unwanted_fields_from_arrest_data_task import remove_unwanted_fields_from_arrest_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'nyc_arrests_and_pop_facts',
    default_args=default_args,
    description='Fetch and integrate on both arrests records in nyc as well as facts data over nyc population',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

fetch_arrest_data_from_api_task = PythonOperator(
    task_id='fetch_arrest_data_from_api',
    python_callable=fetch_arrest_data_from_api,
    dag=dag
)

fetch_pop_facts_xlsx_file_task = PythonOperator(
    task_id='fetch_pop_facts_xlsx_file',
    python_callable=fetch_pop_facts_xlsx_file,
    dag=dag
)

convert_xlsx_to_json_task = PythonOperator(
    task_id='convert_xlsx_to_json',
    python_callable=convert_xlsx_to_json,
    dag=dag
)

remove_entries_with_minTotalPop_task = PythonOperator(
    task_id='remove_entries_with_minTotalPop',
    python_callable=remove_entries_with_minTotalPop,
    dag=dag
)

remove_entries_with_wrong_geoType_task = PythonOperator(
    task_id='remove_entries_with_wrong_geoType',
    python_callable=remove_entries_with_wrong_geoType,
    dag=dag
)

remove_useless_columns_and_rename_task = PythonOperator(
    task_id='remove_useless_columns_and_rename',
    python_callable=remove_useless_columns_and_rename,
    dag=dag
)

validate_coordinates_task = PythonOperator(
    task_id='validate_coordinates',
    python_callable=validate_coordinates,
    dag=dag
)

derive_geoid_task = PythonOperator(
    task_id='derive_geoid',
    python_callable=derive_geoid,
    dag=dag
)

remove_unwanted_fields_from_arrest_data_task = PythonOperator(
    task_id='remove_unwanted_fields_from_arrest_data',
    python_callable=remove_unwanted_fields_from_arrest_data,
    dag=dag
)

create_star_schema_task = PythonOperator(
    task_id='create_star_schema',
    python_callable=create_star_schema,
    dag=dag
)

load_database_task = PythonOperator(
    task_id='load_database',
    python_callable=load_database,
    dag=dag
)

fetch_arrest_data_from_api_task >> validate_coordinates_task >> derive_geoid_task >> remove_unwanted_fields_from_arrest_data_task

fetch_pop_facts_xlsx_file_task >> convert_xlsx_to_json_task >> remove_entries_with_minTotalPop_task >> remove_entries_with_wrong_geoType_task >> remove_useless_columns_and_rename_task

[remove_unwanted_fields_from_arrest_data_task, remove_useless_columns_and_rename_task, create_star_schema_task] >> load_database_task