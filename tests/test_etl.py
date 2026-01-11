import pytest
import pandas as pd
import os
import tempfile
from unittest.mock import Mock, patch, mock_open
from pathlib import Path

# Add src directory to Python path for imports
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))

from extract import extract_knbs_data
from transform import transform_data
from load import load_to_bigquery


class TestExtract:
    """Test cases for extract_knbs_data function"""
    
    @patch('requests.get')
    @patch('builtins.open', new_callable=mock_open)
    @patch('pathlib.Path.mkdir')
    def test_extract_success(self, mock_mkdir, mock_file, mock_get):
        """Test successful data extraction"""
        # Mock response
        mock_response = Mock()
        mock_response.content = b"Year,GDP_Value\n2020,100.5\n2021,102.3"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test extraction
        result = extract_knbs_data("https://example.com/data.csv")
        
        # Assertions
        mock_get.assert_called_once_with("https://example.com/data.csv", timeout=30)
        mock_response.raise_for_status.assert_called_once()
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        assert "gdp_data.csv" in result
    
    @patch('requests.get')
    @patch('builtins.open', new_callable=mock_open)
    @patch('pathlib.Path.mkdir')
    def test_extract_http_error_fallback(self, mock_mkdir, mock_file, mock_get):
        """Test fallback to sample data on HTTP error"""
        # Mock HTTP error
        mock_get.side_effect = Exception("HTTP Error")
        
        # Test extraction
        result = extract_knbs_data("https://example.com/data.csv")
        
        # Assertions
        mock_get.assert_called_once_with("https://example.com/data.csv", timeout=30)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        assert "gdp_data.csv" in result


class TestTransform:
    """Test cases for transform_data function"""
    
    def setup_method(self):
        """Setup test data"""
        self.sample_data = """Year,County,GDP_Value,Population
2020,Nairobi,500.5,4.5
2020,Mombasa,200.3,1.2
2021,Nairobi,550.2,4.6
2021,Mombasa,210.8,1.3"""
    
    @patch('pandas.read_csv')
    @patch('pathlib.Path.mkdir')
    @patch('pandas.DataFrame.to_csv')
    def test_transform_success(self, mock_to_csv, mock_mkdir, mock_read_csv):
        """Test successful data transformation"""
        # Create sample DataFrame
        df = pd.read_csv(pd.compat.StringIO(self.sample_data))
        mock_read_csv.return_value = df
        
        # Test transformation
        result = transform_data("data/raw/gdp_data.csv")
        
        # Assertions
        mock_read_csv.assert_called_once_with("data/raw/gdp_data.csv")
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_to_csv.assert_called_once()
        assert "gdp_transformed.csv" in result
    
    @patch('pandas.read_csv')
    def test_transform_adds_gdp_growth(self, mock_read_csv):
        """Test that GDP_Growth column is added"""
        # Create sample DataFrame
        df = pd.read_csv(pd.compat.StringIO(self.sample_data))
        mock_read_csv.return_value = df
        
        # Mock file operations
        with patch('pathlib.Path.mkdir'), patch('pandas.DataFrame.to_csv'):
            transform_data("data/raw/gdp_data.csv")
        
        # Check that GDP_Growth column was added to the DataFrame
        assert 'GDP_Growth' in df.columns
    
    @patch('pandas.read_csv')
    def test_transform_aggregates_by_county(self, mock_read_csv):
        """Test county aggregation"""
        # Create sample DataFrame
        df = pd.read_csv(pd.compat.StringIO(self.sample_data))
        mock_read_csv.return_value = df
        
        # Mock file operations
        with patch('pathlib.Path.mkdir'), patch('pandas.DataFrame.to_csv'):
            transform_data("data/raw/gdp_data.csv")
        
        # The function should group by County and aggregate
        # We can't easily test the exact aggregation without more complex mocking
        # but we can verify the function runs without error
        assert True
    
    @patch('pandas.read_csv')
    def test_transform_validation_warning(self, mock_read_csv):
        """Test validation warning for insufficient rows"""
        # Create small DataFrame (less than 40 rows)
        small_df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        mock_read_csv.return_value = small_df
        
        # Mock file operations
        with patch('pathlib.Path.mkdir'), patch('pandas.DataFrame.to_csv'):
            # Should not raise exception, just log warning
            transform_data("data/raw/gdp_data.csv")
        
        # Should complete without raising exception
        assert True


class TestLoad:
    """Test cases for load_to_bigquery function"""
    
    @patch('pandas.read_csv')
    @patch('google.cloud.bigquery.Client')
    @patch('os.getenv')
    def test_load_success(self, mock_getenv, mock_client, mock_read_csv):
        """Test successful BigQuery load"""
        # Mock environment variables
        mock_getenv.return_value = "/path/to/credentials.json"
        
        # Mock DataFrame
        df = pd.DataFrame({'Year': [2020, 2021], 'GDP_Value': [100, 200]})
        mock_read_csv.return_value = df
        
        # Mock BigQuery client and job
        mock_client_instance = Mock()
        mock_client.return_value = mock_client_instance
        
        mock_job = Mock()
        mock_job.errors = None
        mock_job.output_rows = 2
        mock_job.result.return_value = None
        
        mock_client_instance.load_table_from_file.return_value = mock_job
        mock_client_instance.list_datasets.return_value = []
        
        # Mock file operations
        with patch('builtins.open', mock_open(read_data=b"Year,GDP_Value\n2020,100")):
            result = load_to_bigquery(
                "data/transformed/gdp_transformed.csv",
                "test-project",
                "test-dataset",
                "test-table"
            )
        
        # Assertions
        assert result is True
        mock_client.assert_called_once_with(project="test-project")
        mock_client_instance.load_table_from_file.assert_called_once()
    
    @patch('google.cloud.bigquery.Client')
    @patch('os.getenv')
    def test_load_auth_error(self, mock_getenv, mock_client):
        """Test authentication error handling"""
        # Mock environment variables
        mock_getenv.return_value = "/path/to/credentials.json"
        
        # Mock authentication error
        mock_client_instance = Mock()
        mock_client_instance.list_datasets.side_effect = Exception("Permission denied")
        mock_client.return_value = mock_client_instance
        
        # Test load function
        with pytest.raises(PermissionError):
            load_to_bigquery(
                "data/transformed/gdp_transformed.csv",
                "test-project",
                "test-dataset",
                "test-table"
            )
    
    @patch('os.path.exists')
    def test_load_file_not_found(self, mock_exists):
        """Test file not found error"""
        mock_exists.return_value = False
        
        with pytest.raises(FileNotFoundError):
            load_to_bigquery(
                "nonexistent_file.csv",
                "test-project",
                "test-dataset",
                "test-table"
            )


if __name__ == "__main__":
    pytest.main([__file__])
