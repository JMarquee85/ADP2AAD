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
print("Cursor object created.")

# Query variables
sql_query = "SELECT * FROM ANALYTICS.SOURCE.src_adp_persons;"

#run the query
try:
	cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE engineer_wh")
	#cs.execute("show tables like '%%ADP%' in schema analytics.source;")
	#cs.execute("show tables like '%%ADP%' in schema analytics.staging;")
	cs.execute("SELECT * FROM ANALYTICS.source.src_adp_persons")
	cs.execute(sql_query)

	# This is to show all available colums and identify them below as variable names to select the ones to print. 
	# Will want to turn this into a dictionary once the data is parsed in a way that makes sense. 
	#for record in cs:
	#	print(record)

	for a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u in cs:
		#(employee id, first name, last name, city, state)
		employee_id = a
		first_name = c
		last_name = e
		city = o
		state = p
		# Get these variables into the dictionary
		employee_info = {'employee_id': employee_id, 'first_name': first_name, 'last_name': last_name, 'city': city, 'state': state}
		print(employee_info)

	#df = cs.fetch_pandas_all()
	#df.info()
	#print("---------------")
	#print(df.to_string())

finally:
	cs.close()

