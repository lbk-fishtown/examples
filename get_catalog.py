import requests
import json
from requests import api
from requests.api import head
import yaml
import os
import re
import datetime

today = datetime.datetime.now().strftime("%Y%m%d")

# set api token as environment variable or hardcode
# this can be found in your profile in dbt Cloud
api_token = os.getenv('DBT_CLOUD_API_KEY')
# api_token = ''
# set account_id & relevant job_id
# this can be found in the url for the job in dbt Cloud
account_id = 4238
job_id = 12389
run_id = 0

# set headers
headers = {
    'Authorization': f"Token {api_token}",
    'Content-Type': 'application/json'
}

# endpoint to get most recent run details
runs_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/?job_definition_id={job_id}&limit=1&order_by=-id"

# retrieve most recent run, and parse response for run_id
try:
    runs_resp = requests.get(runs_url, headers=headers)
    run_json = json.loads(runs_resp.content)
    run_id = run_json['data'][0]['id']
except Exception as e:
    raise SystemExit(e)
    
# endpoint for catalog
catalog_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/catalog.json"

# catalog data file name
catalog_filename = f"catalog_{today}_{account_id}_{job_id}_{run_id}.json"

# retrieve catalog and attempt to write to disk
try:
    catalog_resp = requests.get(catalog_url, headers=headers)
    # write catalog file to disk
    with open(catalog_filename, "w") as catalogfile:
        catalogfile.write( json.dumps(json.loads(catalog_resp.content), indent=4, sort_keys=False) )
except Exception as e:
    raise SystemExit(e)

