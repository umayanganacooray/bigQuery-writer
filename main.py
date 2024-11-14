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
        
account_info = get_gcloud_account_info()
credentials = service_account.Credentials.from_service_account_info(account_info, scopes=scopes)
client = bigquery.Client(project=project, credentials=credentials)
        

def parse_datetime(date_string):
    if date_string:
        dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
        return dt.isoformat()
    return None

def transform_issue(issue):    
    return {
        "issue_id" : issue.get("id"),
        "issue_title" : issue.get("title"),
        "created_time" : parse_datetime(issue.get("created_at")),
        "updated_time" : parse_datetime(issue.get("updated_at")),
        "labels" : [label.get("name", "") for label in issue.get("labels", [])],
        "assignees" : [assignee.get("login", "") for assignee in issue.get("assignees", [])],
        "state" : issue.get("state"),
        "state_reason" : issue.get("state_reason"),
        "closed_time" : parse_datetime(issue.get("closed_at")) if issue.get("closed_at") else None
    }
     
          
def insert_data(rows):
    table = f"{project}.{dataset}.ISSUE"
    if len(rows) == 0:
        print(f"No rows present to insert into {table}")
        sys.exit(1)

    print(f"Inserting {len(rows)} rows into {table}")
    
    job_config = bigquery.LoadJobConfig(
        create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  
    )
    job_config.schema = client.get_table(table).schema
    
    load_job = client.load_table_from_json(rows, table, job_config=job_config)
    load_job.result()

    print("Rows inserted successfully.")     


def main():
    
    per_page = 100
    page = 1

    path = "/repos/wso2-enterprise/choreo/issues"
    query = "q=state:open"
    
    values = []

    while True:
        url = f"{serviceURL}{path}?{query}&per_page={per_page}&page={page}"
        headers = {"Authorization": f"Bearer {GITHUB_PAT}"}
        response = requests.get(url,headers=headers)

        if response.status_code != 200:
            print("Failed to get issues:", response.status_code)
            break

        issues = response.json()
        if not issues:
            break

        for issue in issues:
            values.append(transform_issue(issue))
        
        page += 1
    insert_data(values)

if __name__ == "__main__":
    main()
