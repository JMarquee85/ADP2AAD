#######################
# Query Snowflake for the latest ADP data#
# Query Microsoft Graph to retrieve a list of all AAD user entities
# Create a dict to look up AAD users by email address#
# Iterate through data from Snowflake#
# Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

# Stuff to do:
# Create a CSV report for various reporting. Create a new one each time, based on date and time of the operation or add time stamps as the 
# process is completed. 
# Report things like users that are deleted, users in ADP that do not have emails listed or emails that do not end in @talkiatry.com (per Megan Hirsch, there might not be a lot we can do on this one as the user can change this field on their own.)
# Any way to store and pass along a photo? This was requested at an early stage. 
# Down the road, there is probably a more efficient way to compare these. Put the MS data into a dataframe instead and compare the dataframes? 
# Seems like pandas might be faster or provide some faster methods. 
# Add threading. 
# https://stackoverflow.com/questions/15143837/how-to-multi-thread-an-operation-within-a-loop-in-python

import snowflake.connector
import requests
import pandas as pd
import json
import msgraphpull
from msgraphpull import *
import logging
import csv



# Lists of things to collect along the way. 
no_status = []
no_ms_dictionary_found = []

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

print("Getting Snowflake JWT token... ")
response = requests.post(TOKEN_URL, data=PAYLOAD)
json_data = json.loads(response.text)
TOKEN = json_data["access_token"]
print("Token obtained!")

# Oh look, some logs.
logging.basicConfig(filename="debug.log", level=logging.INFO) #filemode="w" add to 
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
print("Snowflake cursor object created.\n")

# Query snowflake for the current employee data and sort by last name.
sql_query = """
SELECT * FROM analytics.common.dim_employees
ORDER by EMPLOYEE_LAST_NAME;
"""

try:
    # run the query
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

    # Loop through the Snowflake dataframe and set variables.
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
        employee_status = getattr(row, "EMPLOYEE_STATUS") # Active, Terminated, or Inactive (i.e. on leave)
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

        # Get the dictionary related to this user in MSGraph
        # What's another way we could more readily identify the users? Maybe store their ADP employee ID in AAD?
        if employee_email:
        	return_dict = return_msuser_dict(employee_email.casefold(), employee_id, employee_full_name, employee_preferred_name, ms_users)
        	#print(return_dict) # Aaaaand this is a list, not a dictionary. 
        elif employee_email is None or "@talkiatry.com" not in employee_email: 
        	return_dict = return_msuser_dict(employee_email, employee_id, employee_full_name, employee_preferred_name, ms_users)
        else:
        	return_dict = return_msuser_dict(employee_email, employee_id, employee_full_name, employee_preferred_name, ms_users)


        # Check if user has a termination date. 
        # NOTE: Will there ever be a time where the separation date is put in ahead of time? Should write a check here to see 
        # if separation date matches now or in the past to avoid accidental deletion. 
        if employee_status == "Terminated": 
        	# Check if employee exists in MS. If so, deactivate account and delete. If not, move on. 
        	if return_dict:
        		print(f"User marked termed on {employee_separation_date}")
        		print(f"User found in the MSGraph dictionaries. We would now delete {employee_full_name}!\n")
        		#delete_user(employee_email)
        	else: 
        		print(f"{employee_full_name} termed on {employee_separation_date}, not found in MS info. They should have already been deleted.\n")
        		logging.info(f"{employee_full_name} termed on {employee_separation_date}, not found in MS info. They should have already been deleted.\n")

        # Action if user is Active		
        elif employee_status == "Active" or "Inactive": # Inactive covers users on leave and keep their information updated. 
			##### Compare ADP and MS info. 
			# Because ADP users can choose their own email and change this field, don't compare or change any email addresses. We will handle our own copy of those specific fields. 
            if return_dict:
                compare = user_compare(employee_email, employee_id, employee_full_name, employee_preferred_name, employee_first_name, employee_last_name, employee_department, employee_current_role, employee_supervisor_email, employee_city, employee_state, employee_zip, return_dict)
            	# If the result of user_compare() detects differences in the core information and what is listed in ADP
            	# It would be good to make this more specific in the future, like only change the manager if it's incorrect or change the city and state if they moved. This might require some kind of returns from the function to flag what needs to be changed.  

            	# These all seem to be coming back as matches, when I'm quite certain they are all not. 
                if compare == "update":
                    print(f"Here, we will change the core information for {employee_full_name}.")
                    #update_user(employee_id, employee_full_name, employee_preferred_name, employee_first_name, employee_last_name, employee_email, employee_department, employee_current_role, employee_start_date, employee_separation_date, is_provider, employee_supervisor_name, employee_supervisor_email, employee_city, employee_state, employee_zip)
                    # Add check to see if the manager is the same. Currently just assuming it's different and taking action. 
                    print(f"Here we would update the manager for {employee_full_name}!\n")
                    #update_manager(employee_email, employee_supervisor_name, employee_supervisor_email)
                    # If there are differences, return True and run the function to change the user info in MS. Otherwise, pass.
                else:
                    print(f"All fields stored in ADP match AAD for {employee_full_name}! Not making any changes...\n") 
            else:
                print(f"\nNo MS graph dictionary found for {employee_full_name}!\nStart Date: {employee_start_date}\n(Have they been created in AAD yet? Have we sent the right information to compare?)\n")
                no_graph_output = f"Email: {employee_email}\nName: {employee_full_name}\nPreferred Name: {employee_preferred_name}\nFirst Name: {employee_first_name}\nLast Name: {employee_last_name}\nDepartment: {employee_department}\nTitle: {employee_current_role}\nManager: {employee_supervisor_name}\nCity: {employee_city}\nState: {employee_state}\nZip: {employee_zip}\nHire Date: {employee_start_date}\nADP Employment Status: {employee_status}\n\n"
                print(no_graph_output)
                no_ms_dictionary_found.append(no_graph_output)

        	
        # Action if neither is true. 
        else:
            print(f"employee_status not found for {employee_full_name}!\n")
            logging.info(f"employee_status not found for {employee_full_name}!\n")
            no_status.append(employee_email)
        	


finally:
    cs.close()

    # Print the results of the lists collected in the loop. 
    # The MSDictionary one is almost definitely a name mismatch. I see a lot of users who changed their name. 
    # Maybe include a search by preferred name? Some other tweaks there?
    print("No Graph Output:\n")
    for x in no_ms_dictionary_found:
    	print(x)
    
