# Python skeleton example of how to retrieve Environment & Credentials metadata via dbt Cloud API
# Credentials have an "id" that is referenced by the Environment metadata

import requests
import json
from requests import api
from requests.api import head
import os
import re
import enum

# set api token as environment variable or hardcode
# this can be generate following these instructions https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/service-tokens
api_token = os.getenv('DBT_CLOUD_API_KEY')
#api_token = ''

# set account_id & relevant job_id
# this can be found in the url for the job in dbt Cloud
#   https://cloud.getdbt.com/#/accounts/<ACCOUNT_NUMBER>/projects/<PROJECT_NUMBER>
account_id = 13858
project_id = 36215
base_url = 'https://cloud.getdbt.com'

# set headers
headers = {
    'Authorization': f"Token {api_token}",
    'Content-Type': 'application/json'
}

# EXAMPLE: retrieving credentials & environments 
credentials_url = f"{base_url}/api/v3/accounts/{account_id}/projects/{project_id}/credentials/"
environments_url = f"{base_url}/api/v3/accounts/{account_id}/projects/{project_id}/environments/"

# retrieve most recent runs, and parse response for run_id
try:
    c_result = requests.get(credentials_url, headers=headers)
    c_result = json.loads(c_result.content)

    e_result = requests.get(environments_url, headers=headers)
    e_result = json.loads(e_result.content)
except Exception as e:
    raise SystemExit(e)

# print out credentials & environments metadata
print(json.dumps(c_result, indent=4, sort_keys=True))
print(json.dumps(e_result, indent=4, sort_keys=True))