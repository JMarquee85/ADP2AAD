#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
# import os

# authenticate in using SSO
ctx = snowflake.connector.connect(
	user='josh.marcus@talkiatry.com',
	authenticator='externalbrowser',
	account='pra18133',
	region='us-east-1',
	Role = 'ACCOUNTADMIN',
	#Warehouse = 'SOME_WAREHOUSE'
	)
# Appears this may have worked to auth in. 
# I don't think I have any warehouse access currently. 

cs = ctx.cursor()

sql_query = '''

'''



