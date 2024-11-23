import re
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
project = os.getenv("CHOREO_BIGQUERY_GCLOUD_PROJECT")
dataset = os.getenv("CHOREO_BIGQUERY_GCLOUD_DATASET")
serviceURL = os.getenv("CHOREO_GITHUB_SERVICEURL")
GITHUB_PAT = os.getenv("CHOREO_GITHUB_GITHUB_PAT")
HEADERS = {"Authorization": f"Bearer {GITHUB_PAT}"}

OWNER = "wso2-enterprise"
REPO = "choreo"

def get_gcloud_account_info():
    try:
        gcloud_var: str = os.getenv('CHOREO_BIGQUERY_GCLOUD_ACCOUNT')
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
        
def get_projects():
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/projects"
    response = requests.get(url, headers=HEADERS)
    return response.json()

def get_columns(project_id):
    url = f"https://api.github.com/projects/{project_id}/columns"
    response = requests.get(url, headers=HEADERS)
    return response.json()

def get_cards(column_id):
    url = f"https://api.github.com/projects/columns/{column_id}/cards"
    response = requests.get(url, headers=HEADERS)
    return response.json()

def extract_issue_id(content_url):
    if not content_url: 
        return None
    match = re.search(r"/issues/(\d+)$", content_url)
    if match:
        return int(match.group(1))
    return None

def build_project_structure():
    project_structure = {}
    projects = get_projects()
    for project in projects:
        project_id = project.get("id")
        project_name = project.get("name")
        project_structure[project_id] = {"project_name": project_name, "columns": {}}

        columns = get_columns(project_id)
        for column in columns:
            column_id = column.get("id")
            column_name = column.get("name")
            project_structure[project_id]["columns"][column_id] = {"column_name": column_name, "cards": []}

            cards = get_cards(column_id)
            for card in cards:
                content_url = card.get("content_url")
                issue_id = extract_issue_id(content_url)

                card_info = {
                    "id": card["id"],
                    "issue_id": issue_id
                }
                project_structure[project_id]["columns"][column_id]["cards"].append(card_info)

    return project_structure

def issue_project_mapping():
    project_structure = build_project_structure()
    
    issue_to_projects = {}
    
    for project_id, project_data in project_structure.items():
        project_name = project_data["project_name"]
        
        for column_id, column_data in project_data["columns"].items():
            for card in column_data["cards"]:
                issue_id = card.get("issue_id")
                
                if issue_id: 
                    if issue_id not in issue_to_projects:
                        issue_to_projects[issue_id] = []
                    
                    if project_name not in issue_to_projects[issue_id]:
                        issue_to_projects[issue_id].append(project_name)
    
    return issue_to_projects

def get_projects_names_for_issue(issue_to_projects, issue_id):
    if issue_id in issue_to_projects:
        return issue_to_projects[issue_id]
    else:
        return None 

def transform_issue(issue, projects):   
    def parse_datetime(date_string):
        if date_string:
            return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%S")
        return None
 
    return {
        "issue_id" : issue.get("number"),
        "issue_title" : issue.get("title"),
        "created_time" : parse_datetime(issue.get("created_at")),
        "updated_time" : parse_datetime(issue.get("updated_at")),
        "labels" : [label.get("name", "") for label in issue.get("labels", [])],
        "assignees" : [assignee.get("login", "") for assignee in issue.get("assignees", [])],
        "state" : issue.get("state"),
        # "state_reason" : issue.get("state_reason"),
        "closed_time" : parse_datetime(issue.get("closed_at")),
        "projects" : projects
    }

def insert_data(rows):
    table = f"{project}.{dataset}.ISSUE"
    if len(rows) == 0:
        print(f"No rows present to insert into {table}")
        sys.exit(1)

    print(f"Inserting {len(rows)} rows into {table}")

    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = client.get_table(table).schema
    
    load_job = client.load_table_from_json(rows, table, job_config=job_config)
    load_job.result()

    print("Rows inserted successfully.")  
    
    
def main():
    
    issue_to_projects = issue_project_mapping()
    
    per_page = 100
    page = 1
    
    path = "/repos/wso2-enterprise/choreo/issues"
    query = "state=all"
      
    values = []

    while True:
        url = f"{serviceURL}{path}?{query}&per_page={per_page}&page={page}"
        response = requests.get(url,headers=HEADERS)

        if response.status_code != 200:
            print("Failed to get issues:", response.status_code)
            break

        issues = response.json()
        if not issues:
            break

        for issue in issues:
            if (issue.get("pull_request")):
                continue
            projects = get_projects_names_for_issue(issue_to_projects,issue.get("number"))
            values.append(transform_issue(issue,projects))
        
        page += 1
    insert_data(values)

if __name__ == "__main__":
    main()
