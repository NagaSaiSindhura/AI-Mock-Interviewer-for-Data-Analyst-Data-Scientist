from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import time

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

# Your web scraping functions (unchanged)
def fetch_all_freemium_questions(
    code_type=1,
    first=100,
    is_freemium=True,
    max_pages=10
):
    GRAPHQL_URL = "https://api.stratascratch.com/graphql/"
    query = """
    query CodingQuestions($first: Int!, $offset: Int!, $codeType: Int!, $isFreemium: Boolean) {
      allEducationalQuestions(
        first: $first,
        offset: $offset,
        codeType: $codeType,
        isFreemium: $isFreemium
      ) {
        totalCount
        edges {
          node {
            id
            questionShort
            difficulty
            url
          }
        }
      }
    }
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Token 88a292d25bfccd0699f2ac319c4c62f788ad477c"
    }

    all_edges = []
    offset = 0

    for page_idx in range(max_pages):
        variables = {
            "first": first,
            "offset": offset,
            "codeType": code_type,
            "isFreemium": is_freemium
        }

        payload = {
            "operationName": "CodingQuestions",
            "variables": variables,
            "query": query
        }

        resp = requests.post(GRAPHQL_URL, headers=headers, data=json.dumps(payload))
        if resp.status_code != 200:
            print(f"Error fetching questions: {resp.status_code} {resp.text}")
            break

        data = resp.json()
        block = data["data"]["allEducationalQuestions"]
        edges = block["edges"]
        total_count = block["totalCount"]

        all_edges.extend(edges)
        print(f"Fetched {len(edges)} questions this page (offset={offset}), total so far={len(all_edges)} / {total_count}")

        if len(all_edges) >= total_count:
            break

        offset += first
        time.sleep(1)

    return all_edges

def fetch_question_details(slug, code_type=1):
    GRAPHQL_URL = "https://api.stratascratch.com/graphql/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Token 88a292d25bfccd0699f2ac319c4c62f788ad477c"
    }

    query = """
    query EducationalQuestionDetails($slug: String!, $codeType: Int!) {
      educationalQuestion(slug: $slug) {
        id
        questionShort
        question
        difficulty
        companies {
          name
        }
        tables(codeType: $codeType)
        officialOrRecommendedSolution(codeType: $codeType) {
          code
        }
        walkthrough(codeType: $codeType)
      }
    }
    """

    variables = {
        "slug": slug,
        "codeType": code_type
    }

    payload = {
        "operationName": "EducationalQuestionDetails",
        "variables": variables,
        "query": query
    }

    resp = requests.post(GRAPHQL_URL, headers=headers, data=json.dumps(payload))
    if resp.status_code != 200:
        print(f"Error fetching details for slug={slug}: {resp.status_code} {resp.text}")
        return None

    data = resp.json()["data"]["educationalQuestion"]
    if not data:
        return None
    
    record = {}
    record["question_id"] = data["id"]
    record["question_title"] = data["questionShort"]
    record["companies"] = "; ".join([c["name"] for c in data["companies"]]) if data["companies"] else ""
    record["question"] = data["question"]
    record["question_tables"] = json.dumps(data["tables"]) if data["tables"] else ""
    record["difficulty"] = data["difficulty"]
    solution_obj = data["officialOrRecommendedSolution"]
    record["solution_code"] = solution_obj["code"] if solution_obj else ""
    record["solution_walkthrough"] = data["walkthrough"] or ""

    return record

def remove_coding_prefix(slug):
    prefix = "/coding/"
    if slug.startswith(prefix):
        return slug[len(prefix):]
    return slug

# Main function to scrape and save to GCS (unchanged)
def scrape_and_save_to_gcs():
    try:
        # Fetch all freemium questions
        question_edges = fetch_all_freemium_questions(first=100, is_freemium=True, max_pages=5)
        
        # For each question, get the slug and fetch extended details
        results = []
        for edge in question_edges:
            node = edge["node"]
            url_field = node.get("url")
            if not url_field:
                continue

            slug = remove_coding_prefix(url_field.strip())
            details = fetch_question_details(slug=slug, code_type=1)
            if details:
                results.append(details)
            time.sleep(0.5)

        # Convert results to DataFrame
        df = pd.DataFrame(results)
        
        # Define GCS output path with the correct bucket name
        output_path = f"gs://strata-scrape/coding_questions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Save to GCS with error handling
        try:
            df.to_csv(output_path, index=False)
            print(f"Done! Wrote {len(results)} rows to {output_path}")
        except Exception as e:
            print(f"Failed to write to GCS: {str(e)}")
            raise
        
    except Exception as e:
        print(f"Error in scrape_and_save_to_gcs: {str(e)}")
        raise

# Define the DAG with updated schedule_interval
with DAG(
    'web_scraping_dag',
    default_args=default_args,
    description='DAG to scrape StrataScratch questions and save CSV to GCS',
    schedule_interval='0 0 1 * *',  # Run at 00:00 on the 1st day of every month
    catchup=False,
) as dag:

    # Define the task
    scrape_task = PythonOperator(
        task_id='scrape_and_save_task',
        python_callable=scrape_and_save_to_gcs,
    )

# Task ordering (single task in this case)
scrape_task