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
		employee_id.append(a)
		hire_date.append(b)
		term_date.append(c)
		employment_status.append(d)
		first_name.append(f)
		last_name.append(g)
		city.append(h)
		state.append(i)
		# Try adding the above to a list of lists, then iterating over the items in the set of lists to create the individual dictionaries. 

	employee_info_lists = []
	employee_info_lists.append(employee_id)
	employee_info_lists.append(hire_date)
	employee_info_lists.append(term_date)
	employee_info_lists.append(employment_status)
	employee_info_lists.append(first_name)
	employee_info_lists.append(last_name)
	employee_info_lists.append(city)
	employee_info_lists.append(state)



	# Iterate over multiple lists:
	# https://www.geeksforgeeks.org/python-iterate-multiple-lists-simultaneously/
	# Then append it to the dictionaries? 

	for a, b, c, d, e, f, g, h in zip(employee_id,hire_date,term_date,employment_status,first_name,last_name,city,state):
		#print (a, b, c, d, e, f, g, h)
		##### STILL NEED MANAGER INFORMATION. 
		defaults = {'employee_id': a, 'first_name': e, 'last_name': f, 'hire_date': b, 'term_date': c, 'employment_status': d, 'city': g, 'state': h}
		## Create a dictionary with the employee_id as the key and the keys from the defaults variable above
		dict = dict.fromkeys(a, defaults)
		employee_info.append(dict)
		##Append the created dictionary to the employee_info dictionary which lives outside the loop
		##Add the other information b-h as values to match keys 

	print(employee_info[0])
	
	# Testing printing the first item of the list of lists above
	# See: https://www.geeksforgeeks.org/python-get-first-element-of-each-sublist/
	# This function pulls the right order of information. 
	# This might be enough to use, or I can use this to iterate through and create a dictionary for each user. 
	#def Extract(employee_info_lists):
		#return [item[200] for item in employee_info_lists]
#
	#print(Extract(employee_info_lists))


	#defaults = {'first_name': '', 'last_name': '', 'hire_date': '', 'term_date': '', 'employment_status': '', 'city': '', 'state': ''}

	#employee_info_dict = dict.fromkeys(employee_id, defaults)

	#Test printing the list of employee numbers
	#print(employee_info_dict)

finally:
	cs.close()

