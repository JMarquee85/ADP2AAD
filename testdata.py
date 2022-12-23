# Test data det for ADP stuff

dict = {
	'@odata.context': 'something',
	'@odata.nextLink': 'something_else',
	'value': [
	{'name': 'John', 'title': 'manager', 'email': 'john@johnco.net'},
	{'name': 'Alan', 'title': 'asst. manager', 'email': 'alan@johnco.net'},
	{'name': 'Tina', 'title': 'Customer Service', 'email': 'tina@johnco.net'},
	{'name': 'Jimmy', 'title': 'Technical Support', 'email': 'jimmy@johnco.net'},
	]
}

#print(dict['value'])

new_dict = dict['value']
# Doing it this way does not create a dictionary, even if what you're setting the variable to is 
# formatted as a dictionary. 
# This might not be the ideal way to format the data. 

print(type(new_dict))