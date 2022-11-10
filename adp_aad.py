#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import pandas

# Create an empty dictionary to hold the employee values
employee_info = {}

# List of key values for the employee_info dictionary
key_list = ["employee_id","first_name","last_name","city","state"]

# Authenticate into Snowflake using SSO
# Work on connecting via OAUTH instead? https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth
ctx = snowflake.connector.connect(
	user='josh.marcus@talkiatry.com',
	authenticator='externalbrowser',
	account='pra18133',
	region='us-east-1',
	#Role = 'engineer_role',
	Warehouse = 'engineer_wh',
	#Database= 'ANALYTICS',
	#Schema= 'SOURCE' 
	)

# Set up a cursor object.
cs = ctx.cursor()
print("\nCursor object created.")

# This query combines the two tables with the needed information and combines them, using the worker ID number to sort. 
sql_query = '''
SELECT w.WORKER_ID, WORKER_ORIGINAL_HIRE_DATE,WORKER_TERMINATION_DATE,WORKER_STATUS, p.PERSON_WORKER_ID, PERSON_LEGAL_GIVEN_NAME, PERSON_LEGAL_FAMILY_NAME_1, PERSON_LEGAL_ADDRESS_CITY_NAME, PERSON_LEGAL_ADDRESS_COUNTRY_SUBDIVISION_LEVEL_1
    FROM ANALYTICS.SOURCE.src_adp_workers as w join ANALYTICS.SOURCE.src_adp_persons as p
        on p.PERSON_WORKER_ID = w.WORKER_ID
    ORDER by w.WORKER_ID, p.PERSON_WORKER_ID;
'''

#run the query
try:
	cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE engineer_wh")
	#cs.execute("show tables like '%%ADP%' in schema analytics.source;")
	#cs.execute("show tables like '%%ADP%' in schema analytics.staging;")
	cs.execute(sql_query)

	### This would show all the available columns:
	#for record in cs:
		#print(record)

	# Set specific variables for certain pieces of information. 
	# The date format currently looks like this: 'hire_date': datetime.date(2022, 4, 11)
	# Would like to get that to a more readable and usable format. 
	for a,b,c,d,e,f,g,h,i in cs:
		employee_id = a
		hire_date = b
		term_date = c
		employment_status = d
		first_name = f
		last_name = g
		city = h
		state = i
		# Adding these assigned variables to a dictionary. 
		employee_info = {'employee_id': employee_id, 'hire_date': hire_date, 'term_date': term_date, 'employment_status': employment_status, 'first_name': first_name, 'last_name': last_name, 'city': city, 'state': state}
		print(employee_info)


	### This may no longer be strictly necessary, handy to keep around as a way to get the information above into a 
	### pandas dataframe. 
	#df = cs.fetch_pandas_all()
	#df.info()
	#print("---------------")
	#print(df.to_string())

finally:
	cs.close()

