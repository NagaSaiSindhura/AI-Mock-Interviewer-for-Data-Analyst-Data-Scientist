from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import string
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from google.cloud import storage

# Download NLTK data (required for stop words and lemmatization)
nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('wordnet')

# Initialize stop words and lemmatizer
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

# Preprocessing functions
def preprocess_text(text):
    """Preprocess text by removing punctuation, stop words, lemmatizing, and converting to lowercase."""
    if not isinstance(text, str) or not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))
    
    # Tokenize
    tokens = word_tokenize(text)
    
    # Remove stop words
    tokens = [word for word in tokens if word not in stop_words]
    
    # Lemmatize
    tokens = [lemmatizer.lemmatize(word) for word in tokens]
    
    # Join tokens back into a string
    return " ".join(tokens)

def get_latest_csv_file(bucket_name, prefix):
    """Get the most recent CSV file from the GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if not blobs:
        raise FileNotFoundError(f"No files found in gs://{bucket_name}/{prefix}")
    
    # Sort blobs by creation time (newest first) and get the latest one
    latest_blob = max(blobs, key=lambda blob: blob.time_created)
    return f"gs://{bucket_name}/{latest_blob.name}"

def preprocess_data():
    try:
        # Define the bucket and prefix for input files
        bucket_name = "strata-scrape"
        input_prefix = "coding_questions_"
        
        # Get the latest CSV file from the bucket
        input_path = get_latest_csv_file(bucket_name, input_prefix)
        print(f"Reading input file: {input_path}")
        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(input_path)
        
        # Step 1: Rename columns
        df = df.rename(columns={
            'question_id': 'sql_id',
            'solution_code': 'answer'
        })
        
        # Step 2: Drop unnecessary columns
        df = df.drop(columns=['question_title', 'solution_walkthrough'], errors='ignore')
        
        # Step 3: Add new columns
        
        # Add 'category' column with value 'SQL'
        df['category'] = 'SQL'
        
        # Step 4: Remove null values
        df = df.dropna()
        
        # Step 5: Remove duplicates
        df = df.drop_duplicates()
        
        # Step 6: Preprocess text columns
        # Preprocess both 'question' and 'answer' columns
        text_columns = ['question', 'answer' ,'companies','question_tables']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(preprocess_text)
        
        # Define the output path for the preprocessed file
        output_path = f"gs://{bucket_name}/preprocessed_questions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Save the preprocessed DataFrame to GCS
        try:
            df.to_csv(output_path, index=False)
            print(f"Done! Wrote {len(df)} rows to {output_path}")
        except Exception as e:
            print(f"Failed to write to GCS: {str(e)}")
            raise
        
    except Exception as e:
        print(f"Error in preprocess_data: {str(e)}")
        raise

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 8),
}

# Define the DAG
with DAG(
    'preprocessing_dag',
    default_args=default_args,
    description='DAG to preprocess StrataScratch questions and save CSV to GCS',
    schedule_interval='0 0 1 * *',  # Run at 00:00 on the 1st day of every month
    catchup=False,
) as dag:

    # Preprocessing task (no dependency on web_scraping_dag)
    preprocess_task = PythonOperator(
        task_id='preprocess_task',
        python_callable=preprocess_data,
    )