#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import pandas

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
	cs.execute("show tables like '%%ADP%' in schema analytics.source;")
	cs.execute("show tables like '%%ADP%' in schema analytics.staging;")
	cs.execute(sql_query)
	df = cs.fetch_pandas_all()
	df.info()
	print("---------------")
	print(df.to_string())

finally:
	cs.close()

#This is working in snowflake as a basic query, but not working in this script as of yet:
#
#USE WAREHOUSE engineer_wh;
#SELECT * FROM analytics.source.src_adp_persons;