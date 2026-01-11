# Kenyan Economic Data ETL

## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline for processing Kenyan economic data from the Kenya National Bureau of Statistics (KNBS). The pipeline focuses on GDP data extraction, transformation, and loading into a data warehouse for analysis and reporting.

## Tech Stack
- **Python**: Core programming language for ETL processes
- **Pandas**: Data manipulation and analysis
- **Apache Airflow**: Workflow orchestration and scheduling
- **Google Cloud BigQuery**: Data warehouse for storing processed data
- **Requests**: HTTP library for API calls and data extraction

## Project Structure
```
kenyan-economic-data-etl/
├── src/
│   └── etl/           # ETL scripts and modules
├── data/              # Raw and processed data files
├── dags/              # Airflow DAG files
├── requirements.txt   # Python dependencies
└── README.md          # Project documentation
```

## Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Google Cloud account with BigQuery enabled
- Apache Airflow installed and configured
- Access to KNBS data sources

### Installation

1. Clone the repository:
```bash
git clone https://github.com/CippyCabana1109/kenyan-economic-data-etl.git
cd kenyan-economic-data-etl
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up Google Cloud credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

### Configuration
Create a `.env` file in the project root with the following variables:
```
GOOGLE_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=kenyan_economic_data
KNBS_API_KEY=your-knbs-api-key
```

## Usage

### Running ETL Scripts
Execute the main ETL pipeline:
```bash
python -m src.etl.main
```

### Airflow DAGs
Place your DAG files in the `dags/` directory. The DAGs will be automatically picked up by Airflow and can be scheduled to run at regular intervals.

### Data Flow
1. **Extract**: Pull economic data from KNBS APIs or data sources
2. **Transform**: Clean, validate, and structure the data using Pandas
3. **Load**: Store processed data in BigQuery tables for analysis

## Development

### Adding New Data Sources
1. Create new extraction modules in `src/etl/extract/`
2. Add transformation logic in `src/etl/transform/`
3. Update loading scripts in `src/etl/load/`

### Testing
Run tests with:
```bash
python -m pytest tests/
```

## Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License
This project is licensed under the MIT License - see the LICENSE file for details.
