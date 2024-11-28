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

graphql_path = "/graphql"
GITHUB_GRAPHQL_API = f"{serviceURL}{graphql_path}"

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
        

def execute_graphql_query(query, variables=None):
    """
    Executes a GraphQL query against the GitHub API.
    """
    response = requests.post(
        GITHUB_GRAPHQL_API,
        headers=HEADERS,
        json={"query": query, "variables": variables}
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"GraphQL query failed with status code {response.status_code}: {response.text}")
        raise Exception("GraphQL query failed")

def fetch_all_projects_with_graphql(owner, repo):
    query = """
    query($owner: String!, $repo: String!, $cursor: String) {
      repository(owner: $owner, name: $repo) {
        projectsV2(first: 20, after: $cursor) {
          nodes {
            id
            title
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
    """
    variables = {"owner": owner, "repo": repo, "cursor": None}
    projects = []

    while True:
        result = execute_graphql_query(query, variables)
        projects_data = result.get("data", {}).get("repository", {}).get("projectsV2", {})
        projects.extend(projects_data.get("nodes", []))

        # Pagination handling
        page_info = projects_data.get("pageInfo", {})
        if page_info.get("hasNextPage"):
            variables["cursor"] = page_info.get("endCursor")
        else:
            break

    return projects

def fetch_project_details(project_id):
    query = """
    query($projectId: ID!, $cursor: String) {
      node(id: $projectId) {
        ... on ProjectV2 {
          id
          title
          items(first: 100, after: $cursor) {
            edges {
              node {
                id
                content {
                  ... on Issue {
                    id
                    number
                    title
                  }
                }
              }
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
      }
    }
    """
    variables = {"projectId": project_id, "cursor": None}
    all_items = []

    while True:
        result = execute_graphql_query(query, variables)
        items = result.get("data", {}).get("node", {}).get("items", {}).get("edges", [])
        all_items.extend(items)

        # Pagination handling
        page_info = result.get("data", {}).get("node", {}).get("items", {}).get("pageInfo", {})
        if page_info.get("hasNextPage"):
            variables["cursor"] = page_info.get("endCursor")
        else:
            break

    return all_items


def issue_project_mapping():
    
    issue_to_projects = {} 
    # Fetch all projects
    projects = fetch_all_projects_with_graphql(OWNER, REPO)
    if projects:
        for project in projects:
            project_name = project['title']

            # Fetch columns and cards for the project
            project_details = fetch_project_details(project["id"])
            if project_details:
                for item in project_details:
                    card_content = item["node"].get("content", {})
                    issue_number = card_content.get("number")
                    issue_title = card_content.get("title")

                    if issue_number and issue_title:
                        if issue_number not in issue_to_projects:
                            issue_to_projects[issue_number] = []
                            
                        if project_name not in issue_to_projects[issue_number]:
                            issue_to_projects[issue_number].append(project_name)
            else:
                print("Failed to fetch project details.", project_name)
    
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
        "projects" : projects,
        "url" : issue.get("html_url")
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
