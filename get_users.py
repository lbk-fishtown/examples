import requests
import json
from requests import api
from requests.api import head
import re


# set api token as environment variable or hardcode
# this can be found in your profile in dbt Cloud
#api_token = os.getenv('DBT_CLOUD_API_KEY')
api_token = ''
# set account_id & relevant job_id
# this can be found in the url for the job in dbt Cloud
account_id = ''
limit_num_of_users = 50

# set headers
headers = {
    'Authorization': f"Token {api_token}",
    'Content-Type': 'application/json'
}

# endpoint for listing runs, limit to most recent XXX runs based on limit_num_of_runs
user_list_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/users/?limit={limit_num_of_users}"

# retrieve most recent runs, and parse response for run_id
try:
    resp = requests.get(user_list_url, headers=headers)
    resp = json.loads(resp.content)
except Exception as e:
    raise SystemExit(e)

# print user details
for user in resp['data']:
    print(user['email'], user['licenses'].get(str(account_id)).get('license_type'), sep='\t')


