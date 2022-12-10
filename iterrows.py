# iterrows examples:

# Printing Stuff

#This prints only the rows with Staff Psychiatrist in the title
for row in df.itertuples(index = True, name='Employees'):
		#print(getattr(row,'Index'),getattr(row,"EMPLOYEE_LAST_NAME")) #Use this to select specific rows 
		role = getattr(row,"EMPLOYEE_CURRENT_ROLE")
		job = "Staff Psychiatrist"
		if job in role:
			print(row, '\n')
		else:
			continue

#Print just email addresses
for row in df.itertuples(name='Emails'):
	email = getattr(row,"EMPLOYEE_EMAIL")
	print (email, '\n')

#Print just me
for row in df.itertuples():
	my_email = getattr(row,"EMPLOYEE_EMAIL")
	if my_email == "josh.marcus@talkiatry.com":
		print(row,'\n')
	else: 
		continue

# Print only terminated users by name. 
for row in df.itertuples(name='Terminated'):
	status = getattr(row,"EMPLOYEE_SEPARATION_DATE")
	name = getattr(row,"EMPLOYEE_FULL_NAME")
	if status != "None":
		print (name)
	else:
		continue 

# Print name and current EMPLOYEE_SEPARATION_DATE
# This is printing users who have None in the EMPLOYEE_SEPARATION_DATE row for some reason.. 
for row in df.itertuples(name='Terminated'):
	status = getattr(row,"EMPLOYEE_SEPARATION_DATE")
	name = getattr(row,"EMPLOYEE_FULL_NAME")
	if status != "None":
		pass 
	else:
		print (status, name)

# Look to see if a user's email address is in a test list and print the row if it matches:
test_names = ["josh.marcus@talkiatry.com","sean.tracey@talkiatry.com","dharmendra.sant@talkiatry.com"]

for row in df.itertuples(name='TestList'):
	email = getattr(row,"EMPLOYEE_EMAIL")
	if email in test_names:
		print (row, '\n')
	else:
		continue 
# Successful. This only outputs the three users that match the email list. 

# The following block was to test if email addresses were contained in a list of names. 
# This might be a similar approach when comparing what we get out of MG Graph. 

test_names = ["josh.marcus@talkiatry.com","sean.tracey@talkiatry.com","dharmendra.sant@talkiatry.com","georgia.gaveras@talkiatry.com","namrata.shah@talkiatry.com"]

	# Use itertuples method (faster)
	# index defaults to true and will return the index in the dataframe. Set to false, it will remove the index.
	# name sets the name of each tuple that is shown
	for row in df.itertuples(name='TestList'):
		# Can store this in a separate file later for readability- see variables.py
		employee_id = getattr(row, 'EMPLOYEE_ID')
		employee_full_name = getattr(row, 'EMPLOYEE_FULL_NAME')
		employee_preferred_name = getattr(row, 'EMPLOYEE_PREFERRED_NAME')
		employee_email = getattr(row, 'EMPLOYEE_EMAIL')
		employee_department = getattr(row, 'EMPLOYEE_DEPARTMENT_OU_NAME_PREFERRED')
		employee_current_role = getattr(row, 'EMPLOYEE_CURRENT_ROLE')
		employee_current_start_date = getattr(row, 'EMPLOYEE_CURRENT_START_DATE')
		empoloyee_separation_date = getattr(row, 'EMPLOYEE_SEPARATION_DATE')
		is_provider = getattr(row, 'IS_EMPLOYEE_CLINICAL_PROVIDER') #1 for yes, 0 for no
		employee_supervisor_name = getattr(row, 'EMPLOYEE_SUPERVISOR_NAME')
		employee_supervisor_email = getattr(row, 'EMPLOYEE_SUPERVISOR_EMAIL')
		employee_city = getattr(row, 'EMPLOYEE_CITY')
		employee_state = getattr(row, 'EMPLOYEE_STATE')
		employee_zip = getattr(row, 'EMPLOYEE_ZIP')
		employee_start_date = getattr(row, 'EMPLOYEE_CURRENT_START_DATE')
		employee_separation_date = getattr(row, 'EMPLOYEE_SEPARATION_DATE') # try testing for this value containing null (Snowflake shows null for current employees here)
		#getattr(row, '')
		#getattr(row, '')
		#getattr(row, '')
		#getattr(row, '')
		#getattr(row, '')
		#getattr(row, '')
		#getattr(row, '')

		# This is currently to filter this out to just a few users. 
		if employee_email in test_names and is_provider == 1:
			#print (row, '\n')
			print('Employee Name: ' + employee_full_name + '\nEmployee Email: ' + employee_email + '\n\n')
		else:
			continue 


