#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
# import os

# Authenticate into Snowflake using SSO
# Work on connecting via OAUTH instead? https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth
ctx = snowflake.connector.connect(
	user='josh.marcus@talkiatry.com',
	authenticator='externalbrowser',
	account='pra18133',
	region='us-east-1',
	Role = 'engineer_role',
	Warehouse = 'engineer_wh' 
	)

# Set up a cursor object.
cs = ctx.cursor()


# Role, warehouse, and test query variables. 

#sql_db = "use database analytics"
sql_query = "SELECT * FROM analytics.source.src_adp_persons;"
#sql_query2 = ""
#sql_query3 = ""

#cs.execute(sql_wh)
#cs.execute(sql_db)
cs.execute(sql_query)
#cs.execute(sql_query2)
#cs.execute(sql_query3)

# Get the data back in a pandas format
#cs.fetch_pandas_all()

#This is working in snowflake as a basic query, but not working in this script as of yet:
#
#USE WAREHOUSE engineer_wh;
#SELECT * FROM analytics.source.src_adp_persons;