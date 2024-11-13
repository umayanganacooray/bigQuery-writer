import sys
import os
import base64
import binascii
import json
import requests
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account  
  
scopes = ["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/bigquery"]
project = os.getenv("CHOREO_BIGQUERY_CONNECTION_FOR_CHOREO_ISSUES_GCLOUD_PROJECT")
dataset = os.getenv("CHOREO_BIGQUERY_CONNECTION_FOR_CHOREO_ISSUES_GCLOUD_DATASET")
serviceURL = os.getenv("CHOREO_GITHUB_SERVICEURL")
GITHUB_PAT = os.getenv("CHOREO_GITHUB_GITHUB_PAT")

def get_gcloud_account_info():
    try:
        gcloud_var: str = os.getenv('CHOREO_BIGQUERY_CONNECTION_FOR_CHOREO_ISSUES_GCLOUD_ACCOUNT')
        return json.loads(base64.b64decode(gcloud_var).decode("utf-8"))
    except KeyError:
        print("You must first set the GCLOUD_ACCOUNT environment variable")
        sys.exit(1)
    except binascii.Error:
        print("Error when decoding GCloud credentials")
        sys.exit(1)

def convert_to_my_datetime(github_date):
    if github_date is None:
        return None
    date = datetime.strptime(github_date, "%Y-%m-%dT%H:%M:%SZ")
    return date.strftime("%Y-%m-%dT%H:%M:%S")

def transform_issue(issue):
    return {
        "issue_id" : issue.get("id"),
        "issue_title" : issue.get("title"),
        "created_time" : convert_to_my_datetime(issue.get("created_at")),
        "labels" : [label["name"] for label in issue.get("labels", [])],
        "assignees" : ", ".join([assignees.get("name", "") for assignees in issue.get("assignees", [])]),
        "state" : issue.get("state"),
        "state_reason" : issue.get("state_reason"),
        "closed_time" : convert_to_my_datetime(issue.get("closed_at"))
    }
       
          
def insert_data(rows):
    table = f"{project}.{dataset}.ISSUE"
    if len(rows) == 0:
        print(f"No rows present to insert into {table}")
        sys.exit(1)

    print(f"Inserting {len(rows)} rows into {table}")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  
    )

    load_job = client.load_table_from_json(rows, table, job_config=job_config)
    load_job.result()

    print("Rows inserted successfully.")     



account_info = get_gcloud_account_info()
credentials = service_account.Credentials.from_service_account_info(account_info, scopes=scopes)
client = bigquery.Client(project=project, credentials=credentials)

def main():
    per_page = 100
    page = 1
    
    path = "/search/issues"
    query = "q=repo:wso2-enterprise/choreo+is:issue"
    
    values = []

    while True:
        url = f"{serviceURL}{path}?{query}&per_page={per_page}&page={page}"
        headers = {"Authorization": f"Bearer {GITHUB_PAT}"}
        response = requests.get(url,headers=headers)

        if response.status_code != 200:
            print("Failed to get issues:", response.status_code)
            break

        issues = response.json().get("items", [])
        if not issues:
            break

        for issue in issues:
            values.append(transform_issue(issue))
        

        page += 1
    insert_data(values)

if __name__ == "__main__":
    main()
