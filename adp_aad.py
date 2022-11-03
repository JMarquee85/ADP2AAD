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
ctx = snowflake.connector.connect(
	user='josh.marcus@talkiatry.com',
	authenticator='externalbrowser',
	account='pra18133',
	region='us-east-1',
	Role = 'engineer_role',
	#Warehouse = 'data_wh'
	)


# Set up a cursor object.
cs = ctx.cursor()


# Test SQL query
sql_query = '''
use role data_role;

use warehouse data_wh;

use database analytics;

show tables like '%ADP%' in schema analytics.source;

show tables like '%ADP%' in schema analytics.staging;
'''



