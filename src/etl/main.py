"""
Main entry point for the Kenyan Economic Data ETL Pipeline.

This script orchestrates the complete ETL process:
1. Extract data from KNBS sources
2. Transform and clean the data
3. Load to BigQuery data warehouse
4. Validate and report results

Usage:
    python -m src.etl.main
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional

from extract import extract_knbs_data
from transform import transform_data
from load import load_to_bigquery


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure logging for the ETL pipeline."""
    logger = logging.getLogger("kenyan_etl")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def run_etl_pipeline(
    knbs_url: str = "https://example-knbs-gdp.csv",
    project_id: str = "your-project-id",
    dataset_id: str = "economic_data",
    table_id: str = "kenyan_gdp",
    log_level: str = "INFO"
) -> bool:
    """
    Run the complete ETL pipeline with error handling and logging.
    
    Args:
        knbs_url: URL for KNBS data source
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        bool: True if pipeline succeeded, False otherwise
    """
    logger = setup_logging(log_level)
    start_time = time.time()
    
    try:
        logger.info("🚀 Starting Kenyan Economic Data ETL Pipeline")
        logger.info(f"Configuration: project={project_id}, dataset={dataset_id}, table={table_id}")
        
        # Step 1: Extract
        logger.info("📥 Step 1: Extracting data from KNBS")
        extract_start = time.time()
        
        try:
            raw_data_path = extract_knbs_data(knbs_url)
            extract_time = time.time() - extract_start
            logger.info(f"✅ Extraction completed in {extract_time:.2f} seconds")
            logger.info(f"📁 Raw data saved to: {raw_data_path}")
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            return False
        
        # Step 2: Transform
        logger.info("🔄 Step 2: Transforming data")
        transform_start = time.time()
        
        try:
            transformed_data_path = transform_data(raw_data_path)
            transform_time = time.time() - transform_start
            logger.info(f"✅ Transformation completed in {transform_time:.2f} seconds")
            logger.info(f"📁 Transformed data saved to: {transformed_data_path}")
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            return False
        
        # Step 3: Load
        logger.info("📤 Step 3: Loading data to BigQuery")
        load_start = time.time()
        
        try:
            load_success = load_to_bigquery(
                transformed_data_path, project_id, dataset_id, table_id
            )
            load_time = time.time() - load_start
            
            if load_success:
                logger.info(f"✅ Load completed in {load_time:.2f} seconds")
                logger.info(f"🗄️ Data loaded to: {project_id}.{dataset_id}.{table_id}")
            else:
                logger.error("❌ Load completed with errors")
                return False
        except Exception as e:
            logger.error(f"❌ Load failed: {e}")
            return False
        
        # Step 4: Validation
        logger.info("✅ Step 4: Validating pipeline results")
        total_time = time.time() - start_time
        
        logger.info("🎉 Pipeline completed successfully!")
        logger.info(f"⏱️ Total execution time: {total_time:.2f} seconds")
        logger.info(f"📊 Summary:")
        logger.info(f"   - Extraction: {extract_time:.2f}s")
        logger.info(f"   - Transformation: {transform_time:.2f}s")
        logger.info(f"   - Loading: {load_time:.2f}s")
        logger.info(f"   - Total: {total_time:.2f}s")
        
        return True
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrupted by user")
        return False
    except Exception as e:
        logger.error(f"💥 Unexpected pipeline error: {e}")
        return False


def main():
    """Main entry point for the ETL pipeline."""
    import os
    
    # Configuration from environment variables
    knbs_url = os.getenv("KNBS_API_URL", "https://example-knbs-gdp.csv")
    project_id = os.getenv("GOOGLE_PROJECT_ID", "your-project-id")
    dataset_id = os.getenv("BIGQUERY_DATASET", "economic_data")
    table_id = os.getenv("BIGQUERY_TABLE", "kenyan_gdp")
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_file = os.getenv("LOG_FILE", "logs/etl_pipeline.log")
    
    # Run pipeline
    success = run_etl_pipeline(
        knbs_url=knbs_url,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        log_level=log_level
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
