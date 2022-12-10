#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import pandas as pd

# Authenticate into Snowflake using SSO
# Work on connecting via OAUTH instead? https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth
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

#run the query
try:
	cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE engineer_wh")
	cs.execute(sql_query)
	results = cs.fetch_pandas_all()

	# Setting dataframe display if printed in the terminal. 
	pd.set_option('display.max_rows', None) 
	pd.set_option('display.max_columns', 500)
	pd.set_option('display.width', 1000)

	sql_results = pd.DataFrame(
	results,
	columns=[col[0] for col in cs.description],)	

	df = sql_results

	#print(sql_results)

	# This works to print a single column. 
	#print(sql_results['EMPLOYEE_SUPERVISOR_NAME'])

	# Now print a row. 
	#print(sql_results.loc[[40]]) #This specifies a specific index number in the pandas dataframe. 

	# Taken from: https://www.learndatasci.com/solutions/how-iterate-over-rows-pandas/
	# Use iterrow method
	#for index, row in df.iterrows():
	#	print (row, '\n')

	test_names = ["josh.marcus@talkiatry.com","sean.tracey@talkiatry.com","dharmendra.sant@talkiatry.com"]

	# Use itertuples method (faster)
	# index defaults to true and will return the index in the dataframe. Set to false, it will remove the index.
	# name sets the name of each tuple that is shown
	for row in df.itertuples(name='TestList'):
		# Can store this in a separate file later for readability 
		employee_id = getattr(row, 'EMPLOYEE_ID')
		employee_full_name = getattr(row, 'EMPLOYEE_FULL_NAME')
		employee_preferred_name = getattr(row, 'EMPLOYEE_PREFERRED_NAME')
		employee_email = getattr(row, 'EMPLOYEE_EMAIL')
		employee_current_role = getattr(row, 'EMPLOYEE_CURRENT_ROLE')
		employee_current_start_date = getattr(row, 'EMPLOYEE_CURRENT_START_DATE')
		empoloyee_separation_date = getattr(row, 'EMPLOYEE_SEPARATION_DATE')
		getattr(row, '')
		getattr(row, '')
		getattr(row, '')
		getattr(row, '')
		getattr(row, '')
		getattr(row, '')
		getattr(row, '')

		# This is currently to filter this out to just a few users. 
		if employee_email in test_names:
			
	
			print (row, '\n')
		else:
			continue 




	# Loop through each row in the data frame. 
	# https://sparkbyexamples.com/pandas/pandas-iterate-over-columns-of-dataframe-to-run-regression/
	#for colname, colval in df.items():
	#	print(colname, colval.values)

	# Get the Graph information into a dictionary or dataframe or something and test if employee email is in that dataframe. 
	# If it is, print "josh.marcus@talkiatry.com found!" or something to that effect. 

	# Iterate through some of the rows in the above dataframe. 



finally:
	cs.close()

