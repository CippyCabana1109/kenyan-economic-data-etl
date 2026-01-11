import requests
import os
from pathlib import Path


def extract_knbs_data(url: str) -> str:
    """
    Extract Kenyan economic data from a URL and save to local file.
    
    This function downloads CSV data from the specified KNBS URL and saves it
    to the local data directory. If the HTTP request fails, it falls back to
    creating sample data for testing purposes.
    
    Args:
        url: URL to download CSV data from (typically KNBS economic data)
        
    Returns:
        str: Absolute path to the saved CSV file
        
    Raises:
        requests.RequestException: If HTTP request fails and fallback also fails
        OSError: If file operations fail during saving
        Exception: For other unexpected errors during extraction
        
    Example:
        >>> file_path = extract_knbs_data("https://example-knbs-gdp.csv")
        >>> print(f"Data saved to: {file_path}")
    """
    print(f"Starting data extraction from: {url}")
    
    # Create data/raw directory if it doesn't exist
    raw_data_dir = Path("data/raw")
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    
    # Define output file path
    output_file = raw_data_dir / "gdp_data.csv"
    
    try:
        # Make HTTP request
        print("Downloading data...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Save data to file
        print(f"Saving data to: {output_file}")
        with open(output_file, 'wb') as f:
            f.write(response.content)
            
        print(f"Successfully downloaded and saved {len(response.content)} bytes")
        return str(output_file)
        
    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        print("Falling back to local sample data...")
        
        # Fallback to local sample data
        fallback_data = """Year,GDP_Value,GDP_Growth_Rate,Population
2020,95.5,0.1,53.8
2021,98.2,2.8,54.0
2022,102.3,4.2,54.3"""
        
        with open(output_file, 'w') as f:
            f.write(fallback_data)
            
        print(f"Created fallback sample data at: {output_file}")
        return str(output_file)
        
    except OSError as e:
        print(f"File operation failed: {e}")
        raise
        
    except Exception as e:
        print(f"Unexpected error during extraction: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    sample_url = "https://example-knbs-gdp.csv"  # Replace with actual KNBS URL
    try:
        file_path = extract_knbs_data(sample_url)
        print(f"Extraction completed. Data saved to: {file_path}")
    except Exception as e:
        print(f"Extraction failed: {e}")
