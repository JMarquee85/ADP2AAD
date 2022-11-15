# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c
# client_secret_id = b22ec483-d46d-453d-b808-cdf276fc092a

#import libraries
import msal

# Create a token data dictionary to use with these requests
app_id = '8af4e31e-7c65-4ad6-8cf0-3a7d67997947'

# Enter the details of your AAD app registration
client_id = '8af4e31e-7c65-4ad6-8cf0-3a7d67997947'
client_secret = '-yj8Q~~JMWNaPV1hpSl1PvzotPEM5Rz1IUGeqau0'
authority = 'https://login.microsoftonline.com/b22ec483-d46d-453d-b808-cdf276fc092a'
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

print(access_token)

