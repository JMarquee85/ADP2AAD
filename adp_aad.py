#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import pandas as pd
import msgraphpull as msg


# Authenticate into Snowflake using SSO
# Work on connecting via OAUTH instead? https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth
# Auth with azure identity in Python? 
# https://github.com/Azure/azure-sdk-for-python/tree/azure-identity_1.5.0/sdk/identity/azure-identity/
ctx = snowflake.connector.connect(
	user='josh.marcus@talkiatry.com',
	authenticator='externalbrowser',
	account='pra18133',
	region='us-east-1',
	)

# Set up a cursor object.
cs = ctx.cursor()
print("\nCursor object created.")

# Query snowflake for the current employee data and sort by last name.
sql_query = '''
SELECT * FROM analytics.common.dim_employees
ORDER by EMPLOYEE_LAST_NAME;
'''

try:
	#run the query
	cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE engineer_wh")
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
		employee_separation_date = getattr(row, 'EMPLOYEE_SEPARATION_DATE')
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

	#Call the function from ms_graph_pull
	#msg.ms_graph_pull()

	# Now trying to pull in the MS Graph information and will write a block to check for matches in the MS Dict and the ADP data and print the results. 
		if employee_email or employee_full_name in msg.ms_user_info:
			print (f"Match for {employee_full_name} found!")
		else:
			print(f"User {employee_full_name} not found!")

finally:
	cs.close()

