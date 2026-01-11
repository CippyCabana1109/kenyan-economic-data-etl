from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import sys
import os

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))

# Import ETL functions
from extract import extract_knbs_data
from transform import transform_data
from load import load_to_bigquery

# Default arguments for the DAG
default_args = {
    'owner': 'yourname',
    'start_date': datetime(2026, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'kenyan_economic_etl',
    default_args=default_args,
    description='ETL pipeline for Kenyan economic data from KNBS',
    schedule_interval='0 8 * * *',  # Daily at 8 AM EAT (UTC+3)
    catchup=False,
    tags=['etl', 'kenya', 'economics', 'knbs'],
)

# Define file paths
RAW_DATA_PATH = 'data/raw/gdp_data.csv'
TRANSFORMED_DATA_PATH = 'data/transformed/gdp_transformed.csv'

# Configuration
KNBS_DATA_URL = 'https://example-knbs-gdp.csv'  # Replace with actual KNBS URL
PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID', 'your-project-id')
DATASET_ID = 'economic_data'
TABLE_ID = 'kenyan_gdp'

def extract_function(**context):
    """Extract data from KNBS and return file path via XCom"""
    print("Starting extraction task")
    file_path = extract_knbs_data(KNBS_DATA_URL)
    print(f"Extraction completed. File saved to: {file_path}")
    
    # Push file path to XCom
    context['task_instance'].xcom_push(key='raw_file_path', value=file_path)
    return file_path

def transform_function(**context):
    """Transform data and return file path via XCom"""
    print("Starting transformation task")
    
    # Get raw file path from XCom
    raw_file_path = context['task_instance'].xcom_pull(task_ids='extract_task', key='raw_file_path')
    if not raw_file_path:
        raise ValueError("No raw file path received from extract task")
    
    print(f"Transforming file: {raw_file_path}")
    transformed_file_path = transform_data(raw_file_path)
    print(f"Transformation completed. File saved to: {transformed_file_path}")
    
    # Push transformed file path to XCom
    context['task_instance'].xcom_push(key='transformed_file_path', value=transformed_file_path)
    return transformed_file_path

def load_function(**context):
    """Load data to BigQuery"""
    print("Starting load task")
    
    # Get transformed file path from XCom
    transformed_file_path = context['task_instance'].xcom_pull(task_ids='transform_task', key='transformed_file_path')
    if not transformed_file_path:
        raise ValueError("No transformed file path received from transform task")
    
    print(f"Loading file to BigQuery: {transformed_file_path}")
    success = load_to_bigquery(
        input_path=transformed_file_path,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID
    )
    
    if success:
        print("Load completed successfully")
        return True
    else:
        raise Exception("Load failed")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_function,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_function,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_function,
    dag=dag,
)

# Validation task - query BigQuery to verify data
validation_query = f"""
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT County) as unique_counties,
    MIN(Year) as earliest_year,
    MAX(Year) as latest_year,
    AVG(GDP_Value) as avg_gdp
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
"""

validation_task = BigQueryInsertJobOperator(
    task_id='validation_task',
    configuration={
        "query": {
            "query": validation_query,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": f"{TABLE_ID}_validation"
            },
            "writeDisposition": "WRITE_TRUNCATE"
        }
    },
    dag=dag,
)

# Optional: Add a task to print validation results
def print_validation_results(**context):
    """Print validation results"""
    print("Validation query executed successfully")
    print(f"Results saved to: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_validation")
    print("Validation query:")
    print(validation_query)

print_validation_task = PythonOperator(
    task_id='print_validation_task',
    python_callable=print_validation_results,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> validation_task >> print_validation_task

# Optional: Add cleanup task to remove temporary files
def cleanup_function(**context):
    """Clean up temporary files"""
    import os
    from pathlib import Path
    
    files_to_clean = [RAW_DATA_PATH, TRANSFORMED_DATA_PATH]
    
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"Cleaned up: {file_path}")
            except Exception as e:
                print(f"Failed to clean up {file_path}: {e}")
        else:
            print(f"File not found for cleanup: {file_path}")

cleanup_task = PythonOperator(
    task_id='cleanup_task',
    python_callable=cleanup_function,
    dag=dag,
    trigger_rule='all_done',  # Run cleanup regardless of previous task success
)

# Add cleanup as final task
print_validation_task >> cleanup_task
