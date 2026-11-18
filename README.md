# Twitter CSV ETL Pipeline with Apache Airflow

This project implements a simple ETL (Extract, Transform, Load) pipeline using **Apache Airflow** to automate the processing of Twitter data stored in CSV format.

The pipeline extracts tweet data, performs basic cleaning and transformation, and saves the processed results into a new CSV file.

## Technologies Used

- **Python** — ETL implementation
- **pandas** — Data cleaning and transformation
- **Apache Airflow** — Workflow orchestration and scheduling
- **Docker Compose** — Containerized environment
- **PostgreSQL** — Airflow metadata database
- **Redis** — Message broker for CeleryExecutor

## ETL Pipeline

The workflow consists of three main tasks:

1. **Extract:** Read tweet data from a local CSV file.
2. **Transform:** Remove rows with missing tweet text and calculate the length of each tweet.
3. **Load:** Save the cleaned dataset into a new CSV file.

### Data Flow

```text
tweets.csv
    |
    v
Extract Data
    |
    v
extracted.csv
    |
    v
Transform Data
    |
    v
transformed.csv
    |
    v
Load Data
    |
    v
cleaned_tweets.csv
```

## Airflow DAG

The ETL workflow is orchestrated using an Apache Airflow DAG named `twitter_etl_pipeline`.

- **Schedule:** Daily
- **Operators:** PythonOperator
- **Execution:** Sequential task dependencies
- **Retry policy:** 1 retry with a 1-minute delay
- **Catchup:** Disabled

Airflow manages task execution, scheduling, and monitoring.

## Project Structure

```text
twitter-etl/
├── dags/
│   ├── twitter_dag.py
│   └── twitter_etl.py
├── data/
│   └── tweets.csv
└── docker-compose.yaml
```

The `data/` directory also stores the intermediate and final CSV files generated during execution.

## Getting Started

### 1. Prepare the dataset

Place a CSV file named `tweets.csv` inside the `data/` directory.

The dataset must contain a column named `tweet`.

Example:

```csv
tweet
"Learning data engineering"
"Building an Airflow pipeline"
```

### 2. Configure the environment

The project uses Docker Compose to configure the Airflow environment with PostgreSQL and Redis.

Before running the pipeline, ensure that the Airflow services are initialized and that the required scheduler, webserver, and Celery worker are configured.

The `dags/` and `data/` directories must be accessible inside the Airflow containers.

### 3. Run the pipeline

Once the Airflow environment is configured:

1. Open the Airflow web interface at `http://localhost:8080`.
2. Find the `twitter_etl_pipeline` DAG.
3. Enable the DAG or trigger it manually.
4. Monitor the execution of the Extract, Transform, and Load tasks.

### 4. Check the results

After successful execution, the cleaned dataset is saved to:

```text
data/cleaned_tweets.csv
```

The output contains tweet text and the calculated `tweet_length` feature, with rows containing missing tweet text removed.

## Possible Improvements

- Integrate the Twitter/X API for automated data collection.
- Store processed data in a database.
- Add data validation and quality checks.
- Implement sentiment analysis on tweet text.
- Extend the pipeline with monitoring and error handling.
