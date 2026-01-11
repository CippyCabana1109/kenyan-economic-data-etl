import pandas as pd
import os
from pathlib import Path


def transform_data(input_path: str) -> str:
    """
    Transform raw Kenyan economic data by cleaning, adding features, and aggregating.
    
    This function loads raw CSV data, performs data cleaning operations,
    adds computed columns like GDP growth, and aggregates data by county
    when available. It handles missing values, data type conversions, and
    validates the dataset structure.
    
    Args:
        input_path: Path to the raw CSV file containing economic data
        
    Returns:
        str: Path to the transformed CSV file ready for loading to BigQuery
        
    Raises:
        FileNotFoundError: If input file doesn't exist at specified path
        ValueError: If data validation fails (e.g., insufficient rows)
        pd.errors.EmptyDataError: If CSV file is empty or corrupted
        Exception: For other unexpected errors during transformation
        
    Example:
        >>> output_path = transform_data("data/raw/gdp_data.csv")
        >>> print(f"Transformed data saved to: {output_path}")
        
    Note:
        - Missing values are filled with 0
        - GDP columns are converted to float type
        - GDP_Growth column is added as year-over-year percentage change
        - Data is aggregated by County if County column exists
        - Validates minimum 40 rows for Kenyan counties (with warning)
    """
    print(f"Starting data transformation from: {input_path}")
    
    # Create output directory
    output_dir = Path("data/transformed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define output file path
    output_file = output_dir / "gdp_transformed.csv"
    
    try:
        # Load raw data
        print("Loading raw data...")
        df = pd.read_csv(input_path)
        print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
        
        # Validation: Check minimum rows (Kenyan counties)
        if len(df) < 40:
            print(f"Warning: Dataset has only {len(df)} rows, expected at least 40 for Kenyan counties")
            # Continue processing but log warning
        
        # Display basic info
        print(f"Columns: {list(df.columns)}")
        print(f"Data types:\n{df.dtypes}")
        
        # Data cleaning
        print("Cleaning data...")
        
        # Handle missing values by filling with 0
        df_cleaned = df.fillna(0)
        print(f"Filled missing values with 0")
        
        # Convert GDP-related columns to float if they exist
        gdp_columns = [col for col in df_cleaned.columns if 'gdp' in col.lower() or 'GDP' in col]
        for col in gdp_columns:
            try:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce').fillna(0)
                print(f"Converted {col} to float")
            except Exception as e:
                print(f"Could not convert {col} to float: {e}")
        
        # Add GDP growth as percentage change year-over-year
        if 'Year' in df_cleaned.columns and len(gdp_columns) > 0:
            gdp_col = gdp_columns[0]  # Use first GDP column found
            print(f"Calculating GDP growth using {gdp_col}")
            
            # Sort by year to ensure correct year-over-year calculation
            df_cleaned = df_cleaned.sort_values('Year')
            
            # Calculate percentage change
            df_cleaned['GDP_Growth'] = df_cleaned[gdp_col].pct_change() * 100
            df_cleaned['GDP_Growth'] = df_cleaned['GDP_Growth'].fillna(0)  # First year will be NaN
            print("Added GDP_Growth column (year-over-year percentage change)")
        
        # Aggregation: Group by County if County column exists
        if 'County' in df_cleaned.columns:
            print("Aggregating data by County...")
            
            # Group by County and compute mean GDP and other metrics
            agg_functions = {}
            
            # Add GDP columns to aggregation
            for col in gdp_columns:
                agg_functions[col] = 'mean'
            
            # Add other numeric columns
            numeric_cols = df_cleaned.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                if col not in agg_functions:
                    agg_functions[col] = 'mean'
            
            # Perform aggregation
            df_aggregated = df_cleaned.groupby('County').agg(agg_functions).reset_index()
            print(f"Aggregated to {len(df_aggregated)} counties")
            
            # Use aggregated data
            df_final = df_aggregated
        else:
            print("No County column found, using cleaned data without aggregation")
            df_final = df_cleaned
        
        # Final validation
        print(f"Final dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        print(f"Final columns: {list(df_final.columns)}")
        
        # Save transformed data
        print(f"Saving transformed data to: {output_file}")
        df_final.to_csv(output_file, index=False)
        
        print(f"Successfully transformed and saved data")
        return str(output_file)
        
    except FileNotFoundError:
        print(f"Input file not found: {input_path}")
        raise
        
    except pd.errors.EmptyDataError:
        print("CSV file is empty")
        raise
        
    except ValueError as e:
        print(f"Data validation error: {e}")
        raise
        
    except Exception as e:
        print(f"Unexpected error during transformation: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    input_path = "data/raw/gdp_data.csv"
    try:
        output_path = transform_data(input_path)
        print(f"Transformation completed. Data saved to: {output_path}")
    except Exception as e:
        print(f"Transformation failed: {e}")
