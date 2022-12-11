# QUERYING MS GRAPH
# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c
# client_secret_id = b22ec483-d46d-453d-b808-cdf276fc092a

#import libraries
import msal
import json
import requests #remove this once everything below is put into its own script. 

# Create a token data dictionary to use with these requests
app_id = '8af4e31e-7c65-4ad6-8cf0-3a7d67997947'

# Enter the details of your AAD app registration
client_id = '8af4e31e-7c65-4ad6-8cf0-3a7d67997947'
client_secret = '-yj8Q~~JMWNaPV1hpSl1PvzotPEM5Rz1IUGeqau0'
authority = 'https://login.microsoftonline.com/803a6c90-ec72-4384-b2c0-1a376841a04b'
scope = ['https://graph.microsoft.com/.default']

# Create an MSAL instance providing the client_id, authority and client_credential parameters
client = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)

# First, try to lookup an access token in cache
token_result = client.acquire_token_silent(scope, account=None)

# If the token is available in cache, save it to a variable
if token_result:
  access_token = 'Bearer ' + token_result['access_token']
  print('Access token was loaded from cache')

# If the token is not available in cache, acquire a new one from Azure AD and save it to a variable
if not token_result:
  token_result = client.acquire_token_for_client(scopes=scope)
  access_token = 'Bearer ' + token_result['access_token']
  print('New access token was acquired from Azure AD')

# print(access_token)

########################
# This should be separated into another script or function later on and called in the main body of the script

#import requests # this is called above for the moment. 

# This is great, but I think it is limited due to pagination limits. 

# GET USERS
token = access_token
url = 'https://graph.microsoft.com/v1.0/users?$select=id,displayName,userPrincipalName,mail,jobTitle,Department,usertype'
# See this URL for selecting user properties, as in the above example:
# https://learn.microsoft.com/en-us/graph/api/user-get?view=graph-rest-1.0&tabs=http
headers = {
  'Authorization': token
}

# GETTING GROUP LISTING
# Copy access_token and specify the MS Graph API endpoint you want to call, e.g. 'https://graph.microsoft.com/v1.0/groups' to get all groups in your organization
#token = access_token
#url = 'https://graph.microsoft.com/v1.0/groups'
#headers = {
#  'Authorization': token
#}

# Make a GET request to the provided url, passing the access token in a header
graph_result = requests.get(url=url, headers=headers)

# Print the results in a JSON format
# I actually think this is already a Python dictionary, not JSON. 
ms_dict = graph_result.json()
print(type(ms_dict)) # shows this is class 'dict'
print(ms_dict)

# Use json.loads method to get the output into a Python dictionary 
#ms_users = json.loads(ms_json)

# Putting the output into a list due to running list comprehension on it not being able to use a sting
# to search the values inside. 
#ms_users = []
#ms_users.append(json.dumps(ms_json))

#ms_users = json.dumps(ms_json)

# Print what we have 
#print(ms_users)

# Next will need to parse this JSON into something usable for this script. 

# Create a table of user's object id, email address, and name
# Maybe pandas, maybe not?
# Potentially relevant: https://blog.darrenjrobinson.com/microsoft-graph-using-msal-with-python/

# if a users exists in the dictionary, print the other key value pairs relating to that user. 
#if "Amelia.Trainor@talkiatry.com" in ms_users:
  #print(f"Amelia.Trainor@talkiatry.com is here.") # This works great. 

# See if we can locate specific users within the dictionary using input with this method
# https://bobbyhadz.com/blog/python-print-specific-key-value-pairs-of-dictionary

# This is a dictionary with these keys:
# @odata.context, @odata.nextLink, value
# value contains a list with dictionaries inside it containing all the user information. 

