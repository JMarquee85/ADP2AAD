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