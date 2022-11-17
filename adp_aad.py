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
key_list = []
#create lists for the employee values to append to below
employee_id = []
hire_date = []
term_date = []
employment_status = []
first_name = []
last_name = []
city = []
state = []

employee_info = {}

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
		#print(record)

	# Set specific variables for certain pieces of information. 
	# The date format currently looks like this: 'hire_date': datetime.date(2022, 4, 11)
	# Would like to get that to a more readable and usable format. 

# Or perhaps should create a dictionary with the employee name as the key and the requested information as the values?
# https://stackoverflow.com/questions/71030855/print-details-of-an-employee-by-entering-the-name-of-employee-in-python

	for a,b,c,d,e,f,g,h,i in cs:
		# instead going to append the output to lists and set the lists as the dictionary values
		#employee_id = a
		employee_id.append(a)
		#hire_date = b
		hire_date.append(b)
		#term_date = c
		term_date.append(c)
		#employment_status = d
		employment_status.append(d)
		#first_name = f
		first_name.append(f)
		#last_name = g
		last_name.append(g)
		#city = h
		city.append(h)
		#state = i
		state.append(i)
		# Adding these assigned variables to a dictionary. 
		
	employee_info = {'employee_id': employee_id, 'hire_date': hire_date, 'term_date': term_date, 'employment_status': employment_status, 'first_name': first_name, 'last_name': last_name, 'city': city, 'state': state}

	#print(employee_info)

	#Test printing the list of employee numbers
	print(employee_id)



	status_key = "employment_status"

		# Figuring out how to loop through a dictionary again... 
		#for x in employee_info.values():
			#value = employee_info.get(status_key)
			#if value is None:
			#	f"{status_key} not found!"
			#elif value == "terminated":
			#	print(x)
			#else:
			#	continue



	### This may no longer be strictly necessary, handy to keep around as a way to get the information above into a 
	### pandas dataframe. 
	#df = cs.fetch_pandas_all()
	#df.info()
	#print("---------------")
	#print(df.to_string())

finally:
	cs.close()

