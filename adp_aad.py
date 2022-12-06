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

# Everything is now located here:
# analytics.common.dim_employees

# This query pulls all needed information and joins the tables together, sorting by WorkerID. 
sql_query = '''
SELECT DISTINCT w.WORKER_ID, w.WORKER_STATUS, p.PERSON_LEGAL_GIVEN_NAME, p.PERSON_LEGAL_FAMILY_NAME_1, a.WORK_ASSIGNMENT_JOB_TITLE, m.WORKER_REPORT_TO_SUPERVISOR_WORKER_ID, 
                p.PERSON_LEGAL_ADDRESS_CITY_NAME, p.PERSON_LEGAL_ADDRESS_COUNTRY_SUBDIVISION_LEVEL_1, e.BUSINESS_COMMUNICATION_EMAIL_URI

FROM ANALYTICS.SOURCE.src_adp_workers as w 
JOIN ANALYTICS.SOURCE.src_adp_persons as p
ON w.WORKER_ID=p.PERSON_WORKER_ID
    AND w.WORKER_ID=p.PERSON_WORKER_ID
JOIN ANALYTICS.SOURCE.src_adp_work_assignments as a 
ON w.WORKER_ID=a.WORK_ASSIGNMENT_WORKER_ID
    AND w.WORKER_ID=a.WORK_ASSIGNMENT_WORKER_ID
JOIN ANALYTICS.SOURCE.src_adp_business_communication as e
ON w.WORKER_ID=e.BUSINESS_COMMUNICATION_WORKER_ID
    AND w.WORKER_ID=e.BUSINESS_COMMUNICATION_WORKER_ID
JOIN ANALYTICS.SOURCE.src_adp_worker_report_to as m   
ON w.WORKER_ID=m.WORKER_REPORT_TO_WORKER_ID
    AND w.WORKER_ID=m.WORKER_REPORT_TO_WORKER_ID

ORDER by w.WORKER_ID
'''

#run the query
try:
	cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE engineer_wh")
	#cs.execute(sql_query)
	cs.execute(sql_query)
	results = cs.fetch_pandas_all()

	pd.set_option('display.max_rows', 200000) 
	pd.set_option('display.max_columns', 500)

	sql_results = pd.DataFrame(
	results,
	columns=[col[0] for col in cs.description],)	

	print(sql_results)

	



finally:
	cs.close()

