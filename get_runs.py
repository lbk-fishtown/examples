import requests
import json
from requests import api
from requests.api import head
import os
import re
import enum

# define a class of different dbt Cloud API status responses in integer format
class DbtJobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30

# set api token as environment variable or hardcode
# this can be found in your profile in dbt Cloud
api_token = os.getenv('DBT_CLOUD_API_KEY')
#api_token = ''
# set account_id & relevant job_id
# this can be found in the url for the job in dbt Cloud
account_id = 4238
limit_num_of_runs = 100

# set headers
headers = {
    'Authorization': f"Token {api_token}",
    'Content-Type': 'application/json'
}

# endpoint for listing runs, limit to most recent XXX runs based on limit_num_of_runs
runs_list_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/?order_by=-id&limit={limit_num_of_runs}&include_related=[\"job\"]"

# retrieve most recent runs, and parse response for run_id
try:
    runs_list = requests.get(runs_list_url, headers=headers)
    runs_list = json.loads(runs_list.content)
except Exception as e:
    raise SystemExit(e)

# print run details
for run in runs_list['data']:
    print(run['id'], DbtJobRunStatus(run['status']).name, run['job']['name'], run['started_at'], run['finished_at'], run['duration'], sep='\t')
