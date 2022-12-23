#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import requests
import pandas as pd
import json
from msgraphpull import *
import logging 

# Pull the Microsoft user information from msgraphpull
print(f"Now pulling AAD users via Microsoft Graph...")
ms_graph_pull()

# Snowflake Options 
# This information partially derived from putting the token in at jwt.io
USER = "d919f591-ed37-4df3-aa8d-b757d8a5b8f3"
ACCOUNT = "pra18133" # Changed from pra18133.us-east-1
REGION = "us-east-1"
ROLE = "AAD_ROLE"
DATABASE = "ANALYTICS"
SCHEMA = "COMMON"
WAREHOUSE = "AAD_WH"

# Azure AD options
AUTH_CLIENT_ID = "be49d25b-e84f-4a5b-9726-c616a16ca4c5"
AUTH_CLIENT_SECRET = "nO58Q~hqpy93aKskNwKOrA5tV_7z4YJYH.4QzcB5"
AUTH_GRANT_TYPE = "client_credentials"
SCOPE_URL = 'api://e2b6615d-8867-4196-bf8b-ba61aa2f53fa/.default'
TOKEN_URL = 'https://login.microsoftonline.com/803a6c90-ec72-4384-b2c0-1a376841a04b/oauth2/v2.0/token'
PAYLOAD = "client_id={clientId}&" \
          "client_secret={clientSecret}&" \
          "grant_type={grantType}&" \
          "scope={scopeUrl}".format(clientId=AUTH_CLIENT_ID, clientSecret=AUTH_CLIENT_SECRET, grantType=AUTH_GRANT_TYPE,
                                    scopeUrl=SCOPE_URL)

print("Getting JWT token... ")
response = requests.post(TOKEN_URL, data=PAYLOAD)
json_data = json.loads(response.text)
#print(json_data) # printing the JSON output for debugging purposes. 
TOKEN = json_data['access_token']
#print(TOKEN) # Show me the token, baby. Take this out later. 
print("Token obtained!")

# Oh look, some logs. 
logging.basicConfig(filename="log.log", level=logging.DEBUG	)

# Snowflake connection
print("Connecting to Snowflake... ")
ctx = snowflake.connector.connect(
                user=USER,
                account=ACCOUNT,
                region=REGION,
                role=ROLE,
                authenticator="oauth",
                token=TOKEN,
                warehouse=WAREHOUSE,
                database=DATABASE,
                schema=SCHEMA
                )

# Set up a cursor object.
cs = ctx.cursor()
print("Snowflake cursor object created.")

# Query snowflake for the current employee data and sort by last name.
sql_query = '''
SELECT * FROM analytics.common.dim_employees
ORDER by EMPLOYEE_LAST_NAME;
'''

try:
	#run the query
	#cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE AAD_WH")
	cs.execute(sql_query)
	results = cs.fetch_pandas_all()

	# Setting dataframe display if printed in the terminal. 
	pd.set_option('display.max_rows', None) 
	pd.set_option('display.max_columns', 500)
	pd.set_option('display.width', 1000)

	df = pd.DataFrame(
	results,
	columns=[col[0] for col in cs.description],)	


	# Filter through the pulled Snowflake df, set variables and do stuff. 
	for row in df.itertuples(name='ADPUserList'):
		# Can store this in a separate file later for readability- see variables.py
		employee_id = getattr(row, 'EMPLOYEE_ID')
		employee_full_name = getattr(row, 'EMPLOYEE_FULL_NAME')
		employee_preferred_name = getattr(row, 'EMPLOYEE_PREFERRED_NAME')
		employee_email = getattr(row, 'EMPLOYEE_EMAIL')
		employee_department = getattr(row, 'EMPLOYEE_DEPARTMENT_OU_NAME_PREFERRED')
		employee_current_role = getattr(row, 'EMPLOYEE_CURRENT_ROLE')
		employee_current_start_date = getattr(row, 'EMPLOYEE_CURRENT_START_DATE')
		employee_separation_date = getattr(row, 'EMPLOYEE_SEPARATION_DATE') # To filter for termed users, can use if employee_separation_date is not None:
		is_provider = getattr(row, 'IS_EMPLOYEE_CLINICAL_PROVIDER') #1 for yes, 0 for no
		employee_supervisor_name = getattr(row, 'EMPLOYEE_SUPERVISOR_NAME')
		employee_supervisor_email = getattr(row, 'EMPLOYEE_SUPERVISOR_EMAIL')
		employee_city = getattr(row, 'EMPLOYEE_CITY')
		employee_state = getattr(row, 'EMPLOYEE_STATE')
		employee_zip = getattr(row, 'EMPLOYEE_ZIP')
		employee_start_date = getattr(row, 'EMPLOYEE_CURRENT_START_DATE')

		## This is currently to filter this out to just a few users. 
#		if employee_separation_date is not None:
#			print(f"{employee_full_name},{employee_email}")
#			#print(f"{employee_full_name} is no longer with the company as of {employee_separation_date}\n")
#			#print (row, '\n')
#		else:
#			continue
#			#print(f"Employee Name: {employee_full_name} \nEmployee Email: {employee_email}\n")


	# Now trying to pull in the MS Graph information and will write a block to check for matches in the MS Dict and the ADP data and print the results. 
		if employee_email or employee_full_name in msgraphpull.aad_users:
			print (f"Match for {employee_full_name} found!")
		else:
			print(f"User {employee_full_name} not found!")

finally:
	cs.close()

