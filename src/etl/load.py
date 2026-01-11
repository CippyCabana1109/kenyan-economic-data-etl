import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import os
from pathlib import Path
from typing import Optional


def load_to_bigquery(input_path: str, project_id: str, dataset_id: str, table_id: str) -> bool:
    """
    Load transformed CSV data to BigQuery table with automatic schema detection.
    
    This function loads a transformed CSV file into a BigQuery table, handling
    authentication, schema detection, and partitioning. It automatically creates
    yearly partitions if a Year column is present and provides comprehensive
    error handling for common BigQuery issues.
    
    Args:
        input_path: Path to the transformed CSV file to load
        project_id: Google Cloud project ID where BigQuery resides
        dataset_id: BigQuery dataset ID (will be created if doesn't exist)
        table_id: BigQuery table ID where data will be loaded
        
    Returns:
        bool: True if load was successful, False if completed with errors
        
    Raises:
        FileNotFoundError: If input file doesn't exist at specified path
        GoogleCloudError: If BigQuery operation fails (permissions, quotas, etc.)
        PermissionError: If authentication fails or insufficient permissions
        Exception: For other unexpected errors during loading
        
    Example:
        >>> success = load_to_bigquery(
        ...     "data/transformed/gdp_transformed.csv",
        ...     "my-project",
        ...     "economic_data", 
        ...     "kenyan_gdp"
        ... )
        >>> print(f"Load successful: {success}")
        
    Note:
        - Uses GOOGLE_APPLICATION_CREDENTIALS environment variable for auth
        - Auto-detects schema from CSV structure
        - Creates yearly partitions on Year column if present
        - Overwrites existing data (WRITE_TRUNCATE)
        - Requires BigQuery Data Editor role on the dataset
    """
    print(f"Starting BigQuery load from: {input_path}")
    print(f"Target: {project_id}.{dataset_id}.{table_id}")
    
    try:
        # Check if credentials are set
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_path:
            print("Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
            print("Attempting to use default credentials...")
        
        # Initialize BigQuery client
        print("Initializing BigQuery client...")
        client = bigquery.Client(project=project_id)
        
        # Test authentication
        print("Testing authentication...")
        try:
            datasets = list(client.list_datasets(max_results=1))
            print("Authentication successful")
        except GoogleCloudError as e:
            print(f"Authentication failed: {e}")
            print("Please ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly")
            raise PermissionError(f"BigQuery authentication failed: {e}")
        
        # Load transformed data
        print("Loading transformed data...")
        df = pd.read_csv(input_path)
        print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
        
        # Prepare table reference
        table_ref = client.dataset(dataset_id).table(table_id)
        
        # Configure job
        job_config = bigquery.LoadJobConfig(
            # Auto-detect schema
            autodetect=True,
            # Write disposition: WRITE_TRUNCATE (overwrite) or WRITE_APPEND
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # Source format
            source_format=bigquery.SourceFormat.CSV,
            # Skip header rows
            skip_leading_rows=1,
        )
        
        # Handle partitioning by Year if present
        if 'Year' in df.columns:
            print("Year column detected, configuring time partitioning...")
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.YEAR,
                field="Year"
            )
            print("Configured yearly partitioning on Year column")
        
        # Load data to BigQuery
        print("Starting BigQuery load job...")
        with open(input_path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )
        
        # Wait for job completion
        print("Waiting for job completion...")
        job.result()  # Wait for the job to complete
        
        # Check job result
        if job.errors:
            print(f"Job completed with errors: {job.errors}")
            return False
        
        print(f"Successfully loaded {job.output_rows} rows to BigQuery")
        print(f"Table: {project_id}.{dataset_id}.{table_id}")
        
        # Get table information
        table = client.get_table(table_ref)
        print(f"Table schema: {len(table.schema)} fields")
        print(f"Table size: {table.num_bytes} bytes")
        
        return True
        
    except FileNotFoundError:
        print(f"Input file not found: {input_path}")
        raise
        
    except GoogleCloudError as e:
        print(f"BigQuery error: {e}")
        if "permission" in str(e).lower() or "access" in str(e).lower():
            print("This appears to be an authentication or permission issue")
            print("Please check:")
            print("1. GOOGLE_APPLICATION_CREDENTIALS environment variable")
            print("2. Service account has BigQuery permissions")
            print("3. Project ID is correct")
        raise
        
    except Exception as e:
        print(f"Unexpected error during BigQuery load: {e}")
        raise


def create_dataset_if_not_exists(client: bigquery.Client, dataset_id: str, project_id: str) -> None:
    """
    Create BigQuery dataset if it doesn't exist.
    
    This helper function checks if a dataset exists in the specified project
    and creates it with default settings if it doesn't exist.
    
    Args:
        client: Authenticated BigQuery client instance
        dataset_id: Dataset ID to check/create
        project_id: Google Cloud project ID
        
    Returns:
        None
        
    Raises:
        GoogleCloudError: If dataset creation fails due to permissions or other issues
        
    Example:
        >>> client = bigquery.Client(project="my-project")
        >>> create_dataset_if_not_exists(client, "economic_data", "my-project")
        >>> print("Dataset ready for use")
    """
    try:
        dataset_ref = client.dataset(dataset_id)
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except GoogleCloudError:
        print(f"Creating dataset {dataset_id}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # or your preferred location
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created successfully")


if __name__ == "__main__":
    # Example usage
    input_path = "data/transformed/gdp_transformed.csv"
    project_id = os.getenv("GOOGLE_PROJECT_ID", "your-project-id")
    dataset_id = "economic_data"
    table_id = "kenyan_gdp"
    
    try:
        success = load_to_bigquery(input_path, project_id, dataset_id, table_id)
        if success:
            print("BigQuery load completed successfully")
        else:
            print("BigQuery load completed with errors")
    except Exception as e:
        print(f"BigQuery load failed: {e}")
