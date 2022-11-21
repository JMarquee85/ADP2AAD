#######################
#Query Snowflake for the latest ADP data#
#Query Microsoft Graph to retrieve a list of all AAD user entities
#Create a dict to look up AAD users by email address#
#Iterate through data from Snowflake#
#Locate AAD user for each entry from Snowflake and update via Microsoft Graph (if there are changes)

# https://docs.snowflake.com/en/user-guide/python-connector-example.html

import snowflake.connector
import pandas

#Create the empty employee_info dictionary
#employee_info = {}
employee_info = []

# Need to try this as nested dictionaries. 
# https://www.programiz.com/python-programming/nested-dictionary
# Should look like:
#employee_info = {
	#128736123 : {'name': employee_name, 'hire_date': hire_date, etc...},
	#128907431 : {'name': employee_name, etc...}
#}
# Employee information will then be identifiable by the employee number. 
# in the for loop below, will need to create the dictionaries inside the main employee_info dictionary
# and then append the information into the dictionary, iterating through 

#defaults = {'first_name': e, 'last_name': f, 'hire_date': b, 'term_date': c, 'employment_status': d, 'city': g, 'state': h}
#create lists for the employee values to append to below
employee_id = []
hire_date = []
term_date = []
employment_status = []
first_name = []
last_name = []
city = []
state = []


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

# This query combines the two tables with the needed information and combines them, using the worker ID number to sort. 
# Going to need to combine this statement with some other tables in order to bring everything that we need forward. Adding some items to the notes file. 
sql_query = '''
SELECT w.WORKER_ID, w.WORKER_ORIGINAL_HIRE_DATE,w.WORKER_TERMINATION_DATE,w.WORKER_STATUS, 
        p.PERSON_WORKER_ID, p.PERSON_LEGAL_GIVEN_NAME, p.PERSON_LEGAL_FAMILY_NAME_1, p.PERSON_LEGAL_ADDRESS_CITY_NAME, p.PERSON_LEGAL_ADDRESS_COUNTRY_SUBDIVISION_LEVEL_1, 
        a.WORK_ASSIGNMENT_WORKER_ID, a.WORK_ASSIGNMENT_JOB_TITLE, a.WORK_ASSIGNMENT_JOB_LONG_NAME, a.WORK_ASSIGNMENT_JOB_SHORT_NAME,
        e.BUSINESS_COMMUNICATION_WORKER_ID, e.BUSINESS_COMMUNICATION_EMAIL_URI,
        m.WORKER_REPORT_TO_WORKER_ID, m.WORKER_REPORT_TO_SUPERVISOR_WORKER_ID

FROM ANALYTICS.SOURCE.src_adp_workers as w 
JOIN ANALYTICS.SOURCE.src_adp_persons as p
ON w.WORKER_ID=p.PERSON_WORKER_ID
JOIN ANALYTICS.SOURCE.src_adp_work_assignments as a 
ON w.WORKER_ID=a.WORK_ASSIGNMENT_WORKER_ID
JOIN ANALYTICS.SOURCE.src_adp_business_communication as e
ON w.WORKER_ID=e.BUSINESS_COMMUNICATION_WORKER_ID
JOIN ANALYTICS.SOURCE.src_adp_worker_report_to as m   
ON w.WORKER_ID=m.WORKER_REPORT_TO_WORKER_ID

ORDER by w.WORKER_ID,p.PERSON_WORKER_ID,a.WORK_ASSIGNMENT_WORKER_ID,e.BUSINESS_COMMUNICATION_WORKER_ID,m.WORKER_REPORT_TO_WORKER_ID
'''

#run the query
try:
	cs.execute("USE ROLE engineer_role")
	cs.execute("USE WAREHOUSE engineer_wh")
	cs.execute(sql_query)

	data = cs.fetch_pandas_all()

	with pandas.option_context('display.max_rows', None, 'display.max_columns', None):

		print(data)

	### This would show all the available columns:
	#for record in cs:
		#print(record)

	# The date format currently looks like this: 'hire_date': datetime.date(2022, 4, 11)
	# Would like to get that to a more readable and usable format. 

	#for a,b,c,d,e,f,g,h,i in cs:
		## instead going to append the output to lists and set the lists as the dictionary values
		#employee_id.append(a)
		#hire_date.append(b)
		#term_date.append(c)
		#employment_status.append(d)
		#first_name.append(f)
		#last_name.append(g)
		#city.append(h)
		#state.append(i)
#		
	## Adds the information collected above into one list. Not sure if this is necessary anymore, so may take out in review.  
	#employee_info_lists = []
	#employee_info_lists.append(employee_id)
	#employee_info_lists.append(hire_date)
	#employee_info_lists.append(term_date)
	#employee_info_lists.append(employment_status)
	#employee_info_lists.append(first_name)
	#employee_info_lists.append(last_name)
	#employee_info_lists.append(city)
	#employee_info_lists.append(state)
#
#
## STILL NEED TO PULL AND USE THE MANAGER INFORMATION FROM ADP/ SNOWFLAKE. 
#
	## Iterate over the lists that pulled the information, sort them into dictionaries with the employee id number as the key and a dictionary with the employee information as the value. 
	#for a, b, c, d, e, f, g, h in zip(employee_id,hire_date,term_date,employment_status,first_name,last_name,city,state):
		#employee_stuff = {'first_name': e, 'last_name': f, 'employment_status': d, 'hire_date': b, 'term_date': c, 'city': g, 'state': h}
		#dict = {a: employee_stuff}
		#employee_info.append(dict)
#
	## This removes duplicates. 
	#employee_info_no_duplicates = []
	#[employee_info_no_duplicates.append(x) for x in employee_info if x not in employee_info_no_duplicates]
	## NOTE: There are still some duplicate employee IDs in here, but this appears to be because there are some ADP employee entries that 
	## have two city and state entries. 
#
	## Testing the no duplicate list
	#for x in (employee_info_no_duplicates):
		#print(x)
#
	### Print this out. 
	##for x in (employee_info):
		##print(x)
#
	## A version of the above that utilizes lists for the dictionary value instead of a nested dictionary. 
	##for a, b, c, d, e, f, g, h in zip(employee_id,hire_date,term_date,employment_status,first_name,last_name,city,state):
		##employee_stuff = [e, f, b, c, d, g, h]
		##dict = {a: employee_stuff}
		##employee_info.append(dict)
#
## Okay, I think you're doing unnecessary work above. 
## Go back to trying to get the information into a pandas data frame and manage it that way. 


finally:
	cs.close()

