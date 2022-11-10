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

# Query variables
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
	cs.execute(sql_query)

	### This would show all the available columns:
	#for record in cs:
	#	print(record)

	# Set specific variables for certain pieces of information. 
	#for a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u in cs:
		#employee_id = a
		#first_name = c
		#last_name = e
		#city = o
		#state = p
		## Adding these assigned variables to a dictionary. 
		#employee_info = {'employee_id': employee_id, 'first_name': first_name, 'last_name': last_name, 'city': city, 'state': state}
		#print(employee_info)

		#Next step would be to call WORKER_ID,WORKER_TERMINATION_DATE,WORKER_STATUS,WORKER_ORIGINAL_HIRE_DATE into another dictionary,
		#Compare the two dictionaries and if a match between the employee_id number and the WORKER_IS is found, combine the two dictionaries, adding the additional variables.


	## This may no longer be strictly necessary, handy to keep around as a way to get the information above into a 
	## pandas dataframe. 
	df = cs.fetch_pandas_all()
	df.info()
	print("---------------")
	print(df.to_string())

finally:
	cs.close()

