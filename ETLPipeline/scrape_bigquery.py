# -*- coding: utf-8 -*-
"""strata_load_to_bigquery.py

DAG to load transformed StrataScratch data into BigQuery dataset interview_data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import storage

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='strata_load_to_bigquery',
    default_args=default_args,
    description='Loads transformed StrataScratch CSVs into BigQuery dataset interview_data',
    schedule_interval='0 0 1 * *',  # Run on the 1st of every month at 00:00
    start_date=datetime(2025, 4, 9),  # Start today (April 9, 2025)
    catchup=False,  # Do not backfill past runs
) as dag:

    # Task 1: Fetch the latest transformed CSV from GCS
    def fetch_latest_transformed_file(ti, **kwargs):
        """
        Fetch the most recent transformed_questions_*.csv file from the strata-scrape bucket
        based on creation time. Push the file path to XCom for downstream tasks.
        """
        bucket_name = "strata-scrape"
        prefix = "transformed_questions_"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # List all blobs with the prefix
        blobs = list(bucket.list_blobs(prefix=prefix))
        if not blobs:
            raise FileNotFoundError(f"No files found in gs://{bucket_name}/{prefix}")

        # Find the most recent blob based on creation time
        latest_blob = max(blobs, key=lambda blob: blob.time_created)
        file_path = f"gs://{bucket_name}/{latest_blob.name}"
        print(f"Found latest file: {file_path}, created at {latest_blob.time_created}")

        # Push the file path to XCom
        ti.xcom_push(key='transformed_questions_file', value=file_path)

    fetch_transformed_file = PythonOperator(
        task_id='fetch_transformed_file',
        python_callable=fetch_latest_transformed_file,
        provide_context=True,
    )

    # Task 2: Create BigQuery table (if it doesn't exist)
    create_transformed_questions_table = BigQueryCreateEmptyTableOperator(
        task_id='create_transformed_questions_table',
        dataset_id='interview_data',  # Updated dataset ID
        table_id='transformed_questions',
        schema_fields=[
            {"name": "sql_id", "type": "INTEGER"},
            {"name": "question", "type": "STRING"},
            {"name": "question_tables", "type": "STRING"},
            {"name": "difficulty", "type": "INTEGER"},
            {"name": "answer", "type": "STRING"},
            {"name": "category", "type": "STRING"},
            {"name": "difficulty_group", "type": "STRING"},
            {"name": "difficulty_encoded", "type": "INTEGER"},
            {"name": "has_code_snippet", "type": "INTEGER"},
            {"name": "question_word_count", "type": "INTEGER"},
            {"name": "answer_word_count", "type": "INTEGER"},
            {"name": "company_count", "type": "INTEGER"},
            {"name": "question_keywords", "type": "STRING"},
            {"name": "answer_keywords", "type": "STRING"},
            {"name": "is_augmented", "type": "INTEGER"},
            {"name": "company_activecampaign_amazon", "type": "INTEGER"},
            {"name": "company_airbnb", "type": "INTEGER"},
            {"name": "company_amadeus_expedia_airbnb", "type": "INTEGER"},
            {"name": "company_amazon", "type": "INTEGER"},
            {"name": "company_amazon_doordash", "type": "INTEGER"},
            {"name": "company_amazon_meta", "type": "INTEGER"},
            {"name": "company_apple", "type": "INTEGER"},
            {"name": "company_apple_amazon", "type": "INTEGER"},
            {"name": "company_apple_dell_microsoft", "type": "INTEGER"},
            {"name": "company_apple_google", "type": "INTEGER"},
            {"name": "company_apple_microsoft", "type": "INTEGER"},
            {"name": "company_asana_twitter", "type": "INTEGER"},
            {"name": "company_city_los_angeles", "type": "INTEGER"},
            {"name": "company_city_san_francisco", "type": "INTEGER"},
            {"name": "company_deloitte_google", "type": "INTEGER"},
            {"name": "company_doordash_lyft", "type": "INTEGER"},
            {"name": "company_dropbox_amazon", "type": "INTEGER"},
            {"name": "company_ebay_amazon", "type": "INTEGER"},
            {"name": "company_espn", "type": "INTEGER"},
            {"name": "company_expedia_airbnb", "type": "INTEGER"},
            {"name": "company_forbes", "type": "INTEGER"},
            {"name": "company_general_assembly_kaplan_google", "type": "INTEGER"},
            {"name": "company_glassdoor_salesforce", "type": "INTEGER"},
            {"name": "company_google", "type": "INTEGER"},
            {"name": "company_google_amazon", "type": "INTEGER"},
            {"name": "company_google_netflix", "type": "INTEGER"},
            {"name": "company_instacart_amazon", "type": "INTEGER"},
            {"name": "company_linkedin", "type": "INTEGER"},
            {"name": "company_linkedin_dropbox", "type": "INTEGER"},
            {"name": "company_lyft", "type": "INTEGER"},
            {"name": "company_meta", "type": "INTEGER"},
            {"name": "company_meta_asana", "type": "INTEGER"},
            {"name": "company_meta_salesforce", "type": "INTEGER"},
            {"name": "company_microsoft", "type": "INTEGER"},
            {"name": "company_microsoft_amazon", "type": "INTEGER"},
            {"name": "company_netflix_google", "type": "INTEGER"},
            {"name": "company_salesforce", "type": "INTEGER"},
            {"name": "company_shopify_amazon", "type": "INTEGER"},
            {"name": "company_spotify", "type": "INTEGER"},
            {"name": "company_tesla_google", "type": "INTEGER"},
            {"name": "company_tesla_salesforce", "type": "INTEGER"},
            {"name": "company_walmart_amazon", "type": "INTEGER"},
            {"name": "company_walmart_best_buy_dropbox", "type": "INTEGER"},
            {"name": "company_wine_magazine", "type": "INTEGER"},
            {"name": "company_yelp", "type": "INTEGER"},
        ],
        gcp_conn_id='google_cloud_default',
    )

    # Task 3: Load data from GCS into BigQuery
    load_transformed_questions_to_bq = BigQueryInsertJobOperator(
        task_id='load_transformed_questions_to_bq',
        configuration={
            "load": {
                "sourceUris": ["{{ ti.xcom_pull(task_ids='fetch_transformed_file', key='transformed_questions_file') }}"],
                "destinationTable": {
                    "projectId": "web-scraping-project-456218",
                    "datasetId": "interview_data",  # Updated dataset ID
                    "tableId": "transformed_questions",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": False,
                "allowQuotedNewlines": True,
                "allowJaggedRows": True,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # Task 4: End of pipeline
    end_pipeline = DummyOperator(task_id='end_pipeline')

    # Define task dependencies
    fetch_transformed_file >> create_transformed_questions_table >> load_transformed_questions_to_bq >> end_pipeline