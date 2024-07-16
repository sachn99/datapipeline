
# Data Pipeline Project

## Project Overview

This project involves building an end-to-end data pipeline to ingest raw data into a data warehouse, transform it, and create visualizations for certain metrics. Additionally, it includes developing RESTful APIs for accessing the processed data.

## Prerequisites

- Python 3.x
- PostgreSQL
- dbt

## Installation

1. **Clone the repository:**

   ```bash
   git clone <repository_url>
   cd datapipeline
   ```

2. **Create and activate a virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up the dbt environment:**

   ```bash
   cd noora_health
   dbt deps
   ```

## Usage

1. **Run the data ingestion scripts:**

   ```bash
   python scripts/data_ingestion.py
   ```

2. **Run dbt transformations:**

   ```bash
   cd noora_health
   dbt run
   ```

3. **Start the API server:**

   ```bash
   python api/app.py
   ```
   http://127.0.0.1:5000

   it will open swagger where you can access all api and excute them
  


4. **Generate visualizations:**

   ```bash
   python visualisation/visualize.py


   http://127.0.0.1:8050

   where you can see visualisation wrt given time range
   ```

## Project Structure

```
datapipeline/
│
├── api/                  # Contains the Flask API code
├── dbt-env/              # Contains dbt environment setup files 
├── logs/                 # Log files for tracking processes
├── noora_health/         # dbt project directory
├── scripts/              # Data ingestion and processing scripts
├── sql/                  # Raw SQL scripts
├── visualisation/        # Scripts for generating visualizations
├── requirements.txt      # Python dependencies
└── README.md             # Project documentation
```

# set db cred in 
   .dbt/profile.yml
   script/load_data.py
   api/api_routes.py
