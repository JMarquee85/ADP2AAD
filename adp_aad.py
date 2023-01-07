#######################
# Query Snowflake for the latest ADP data#
# Query Microsoft Graph to retrieve a list of all AAD user entities
# Create a dict to look up AAD users by email address#
# Iterate through data from Snowflake#
# Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import requests
import pandas as pd
import json
import msgraphpull
from msgraphpull import *
import logging
import csv

# Authenticate Microsoft
print("Authenticating into MS Graph...")
ms_auth_token()
# Pull the Microsoft user information from msgraphpull
print(f"Now pulling AAD users via Microsoft Graph...")
ms_users = ms_graph_pull()

# Snowflake Options
USER = "d919f591-ed37-4df3-aa8d-b757d8a5b8f3"
ACCOUNT = "pra18133"
REGION = "us-east-1"
ROLE = "AAD_ROLE"
DATABASE = "ANALYTICS"
SCHEMA = "COMMON"
WAREHOUSE = "AAD_WH"

# Azure AD options
AUTH_CLIENT_ID = "be49d25b-e84f-4a5b-9726-c616a16ca4c5"
AUTH_CLIENT_SECRET = "nO58Q~hqpy93aKskNwKOrA5tV_7z4YJYH.4QzcB5"
AUTH_GRANT_TYPE = "client_credentials"
SCOPE_URL = "api://e2b6615d-8867-4196-bf8b-ba61aa2f53fa/.default"
TOKEN_URL = "https://login.microsoftonline.com/803a6c90-ec72-4384-b2c0-1a376841a04b/oauth2/v2.0/token"
PAYLOAD = (
    "client_id={clientId}&"
    "client_secret={clientSecret}&"
    "grant_type={grantType}&"
    "scope={scopeUrl}".format(
        clientId=AUTH_CLIENT_ID,
        clientSecret=AUTH_CLIENT_SECRET,
        grantType=AUTH_GRANT_TYPE,
        scopeUrl=SCOPE_URL,
    )
)

print("Getting JWT token... ")
response = requests.post(TOKEN_URL, data=PAYLOAD)
json_data = json.loads(response.text)
TOKEN = json_data["access_token"]
print("Token obtained!")

# Oh look, some logs.
logging.basicConfig(filename="debug.log", level=logging.DEBUG, filemode="w")
# logging.basicConfig(filename="info.log", level=logging.INFO)
# logging.basicConfig(filename="error.log", level=logging.ERROR )

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
    schema=SCHEMA,
)

# Set up a cursor object.
cs = ctx.cursor()
print("Snowflake cursor object created.")

# Query snowflake for the current employee data and sort by last name.
sql_query = """
SELECT * FROM analytics.common.dim_employees
ORDER by EMPLOYEE_LAST_NAME;
"""

try:
    # run the query
    # cs.execute("USE ROLE engineer_role")
    cs.execute("USE WAREHOUSE AAD_WH")
    cs.execute(sql_query)
    results = cs.fetch_pandas_all()

    # Setting dataframe display if printed in the terminal.
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)

    df = pd.DataFrame(
        results,
        columns=[col[0] for col in cs.description],
    )

    # Filter through the pulled Snowflake df, set variables and do stuff.
    for row in df.itertuples(name="ADPUserList"):
        # Can store this in a separate file later for readability- see variables.py
        employee_id = getattr(row, "EMPLOYEE_ID")
        employee_full_name = getattr(row, "EMPLOYEE_FULL_NAME")
        employee_preferred_name = getattr(row, "EMPLOYEE_PREFERRED_NAME")
        employee_first_name = getattr(row, "EMPLOYEE_FIRST_NAME")
        employee_last_name = getattr(row, "EMPLOYEE_LAST_NAME")
        employee_email = getattr(row, "EMPLOYEE_EMAIL")
        employee_department = getattr(row, "EMPLOYEE_DEPARTMENT_OU_NAME_PREFERRED")
        employee_current_role = getattr(row, "EMPLOYEE_CURRENT_ROLE")
        employee_start_date = getattr(row, "EMPLOYEE_CURRENT_START_DATE")
        employee_separation_date = getattr(
            row, "EMPLOYEE_SEPARATION_DATE"
        )  # To filter for termed users, can use if employee_separation_date is not None:
        is_provider = getattr(
            row, "IS_EMPLOYEE_CLINICAL_PROVIDER"
        )  # 1 for yes, 0 for no
        employee_supervisor_name = getattr(row, "EMPLOYEE_SUPERVISOR_NAME")
        employee_supervisor_email = getattr(row, "EMPLOYEE_SUPERVISOR_EMAIL")
        employee_city = getattr(row, "EMPLOYEE_CITY")
        employee_state = getattr(row, "EMPLOYEE_STATE")
        employee_zip = getattr(row, "EMPLOYEE_ZIP")

    for user in msgraphpull.aad_users:
    	if type(user) is dict:
     		print(user['userPrincipalName']) # This is good, but stops at Anthony Mazzarulli. Issue with pagination in other script?
     		# Each pagination of the microsoft graph information returns a list with dictionaries and if I write a check to filter for type dict
     		# then the subsequent lists are passed over. 

        #if employee_email in ms_users:
        	#print(f"{employee_email} found!")
        #else:
        	#print(f"Not finding {employee_email}...")

        # if employee found in the ms_users list of dictionaries, run the update_user() function, if not, print a message, add them to the logs.

    #Now trying to pull in the MS Graph information and will write a block to check for matches in the MS Dict and the ADP data and print the results.
    #This is working again, except many of the names it's catching here do exist in AAD. Need to dig a bit into that.
    # if employee_email or employee_full_name in ms_users:
#    #print (f"Match for {employee_full_name} found!")
#     continue
#     else:
#     print(f"User {employee_full_name} not found!")
#     logging.error(f"User {employee_full_name} not found in Microsoft Graph Data!")#

#    print(ms_users)`
#    print(type(ms_users))

	    


        # Write a check to see if the user's email address exists in the MS information at all. Could write a function for this and call it a day.
        # This takes a while to do its thing and the way I have written it here doesn't seem to be the best approach.
        # Look into using list comprehension again and reference the dictionary that you pull at the beginning of this to see if you can run a check against that successfully.
        #does_user_exist(employee_email)
#        if does_user_exist:
#            print(f"{employee_email} exists!")
#        else:
#            print(f"Not able to find {employee_email}...")

    # If employee separation date exists, check MS to see if user exists. If so, change mailbox to shared and then deleted user.

    # If employee email or hire date does not exist, add message to the log and pass

    # If employee separation date does not exist (else), pass the information as normal into update_user()

    # Create a block here that looks for Email == None or email does not contain @talkiatry.com, add them to a CSV, store locally and then create a ticket for HR to deal with this.
    # Also create a method in this program to email HR@Talkiatry.com if this is detected again.

    ##Now trying to pull in the MS Graph information and will write a block to check for matches in the MS Dict and the ADP data and print the results.
    ##This is working again, except many of the names it's catching here do exist in AAD. Need to dig a bit into that.
    # if employee_email or employee_full_name in ms_users:
    ##print (f"Match for {employee_full_name} found!")
    # continue
    # else:
    # print(f"User {employee_full_name} not found!")
    # logging.error(f"User {employee_full_name} not found in Microsoft Graph Data!")


finally:
    cs.close()
