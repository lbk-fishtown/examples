# Python skeleton example of how to create a set of Credential and Environment, then link them in dbt Cloud
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
# api_token = ''

# set account_id & relevant job_id
# this can be found in the url for the job in dbt Cloud
#   https://cloud.getdbt.com/#/accounts/<ACCOUNT_NUMBER>/projects/<PROJECT_NUMBER>
account_id = 13858
project_id = 36215
base_url = 'https://cloud.getdbt.com'

# snowflake creds
username = 'my_user'
password = 'my_password'

# environment details
environment_name = 'My Environment 3.0'
dbt_version = '1.0.0'
creds_id = 0

credentials_create_body = f"""
{{
    "id": null,
    "account_id": {account_id},
    "project_id": {project_id},
    "state": 1,
    "threads": 1,
    "target_name": "default",
    "type": "snowflake",
    "schema": "dbt_username",
    "auth_type": "password",
    "user": "{username}",
    "password": "{password}"
}}
"""

# set headers
headers = {
    'Authorization': f"Token {api_token}",
    'Content-Type': 'application/json'
}

creds_url = f"{base_url}/api/v3/accounts/{account_id}/projects/{project_id}/credentials/"

# create credentials
try:
    result = requests.post(creds_url, headers=headers, data=json.dumps(json.loads(credentials_create_body)))
    result = json.loads(result.content)
except Exception as e:
    raise SystemExit(e)

print(json.dumps(result, indent=4, sort_keys=True))


# get credentials id
creds_id = result['data']['id']

environment_create_body = f"""
{{
    "id": null,
    "account_id": {account_id},
    "project_id": {project_id},
    "name": "{environment_name}",
    "credentials_id": {creds_id},
    "dbt_version": "{dbt_version}",
    "type": "deployment"
}}
"""

environment_url = f"{base_url}/api/v3/accounts/{account_id}/projects/{project_id}/environments/"

# create environment with associated credentials
try:
    result = requests.post(environment_url, headers=headers, data=json.dumps(json.loads(environment_create_body)))
    result = json.loads(result.content)
except Exception as e:
    raise SystemExit(e)

print(json.dumps(result, indent=4, sort_keys=True))