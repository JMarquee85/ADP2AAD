# QUERYING MS GRAPH
# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c
# client_secret_id = b22ec483-d46d-453d-b808-cdf276fc092a

#import libraries
import msal
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
url = 'https://graph.microsoft.com/v1.0/users?$select=displayName,userPrincipalName'
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
print(graph_result.json())

