# QUERYING MS GRAPH
# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c

#import libraries
import msal
import json
import requests #remove this once everything below is put into its own script. 
import pandas as pd



# The function to auth in to MS Graph and pull the user information. 
def ms_graph_pull():

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
    print('New access token acquired from Azure AD!')  

  ######################## 

  # GET USERS
  token = access_token
  #url = 'https://graph.microsoft.com/v1.0/users?$top=100'
  url = 'https://graph.microsoft.com/v1.0/users?$select=id,displayName,userPrincipalName,manager,mail,jobTitle,Department,usertype'
  # Previously the URL was this to select specific items: https://graph.microsoft.com/v1.0/users?$select=id,displayName,userPrincipalName,mail,jobTitle,Department,usertype

  # See this URL for selecting user properties, as in the above example:
  # https://learn.microsoft.com/en-us/graph/api/user-get?view=graph-rest-1.0&tabs=http
  headers = {
    'Authorization': token
  } 

  # Make a GET request to the provided url, passing the access token in a header
  graph_result = requests.get(url=url, headers=headers) 

  ms_dict = graph_result.json() #dict

  # Test printing/ statements
  #print(type(ms_dict)) # 'dict'
  #print(ms_dict)
  #print(ms_dict['@odata.nextLink'])

  global aad_users
  aad_users = ms_dict['value'] #list

  #print(type(ms_dict))
  #print(aad_users)
  #print(type(aad_users)) 

 # Trying to cycle through the ms graph user information with pagination. 
  print(f"Paginating Microsoft Graph output...")

  while '@odata.nextLink' in ms_dict:
    if url is None:
      print("No next link found.")
    else:
      url = ms_dict.get('@odata.nextLink') 
      graph_result = requests.get(url=url, headers=headers) 
      ms_dict = graph_result.json()
      #print(ms_dict['@odata.nextLink'])
      # Add something here that removes Service Accounts and Vendors. Something like:
      # if department != "Service Account" or "Vendor" or "Shared Mailbox":
      aad_users.append(ms_dict['value'])

  #print(aad_users)
  return aad_users

  #print(type(aad_users))
  #print(len(aad_users))

  # Creating a list of just users using some list comprehension. 
  # https://stackoverflow.com/questions/46448278/extracting-dictionary-items-embedded-in-a-list
  #just_usernames = [i['id'] for i in aad_users]
  #just_usernames = list(map(lambda x: x['id'], aad_users))
  #print(just_usernames)

  # Run it
#ms_graph_pull()

###################################

# Write a class version of the below as well. 

## A function to update the users with Graph API commands.
# Will likely pass the ADP information into this and compare it to the Graph information. 
def update_user(emp_id, full_name, preferred_name, email, department, current_role, start_date, term_date, is_provider, manager, manager_email, city, state, zip_code):
    print(f"ID: {emp_id}\nFull Name: {full_name}\nPreferred Name: {preferred_name}\nEmail: {email}\nDepartment: {department}\nJob Title: {current_role}\nStart Date: {start_date}\nTermination Date: {term_date}\nProvider: {is_provider}\nManager: {manager}\nManager Email: {manager_email}\nCity: {city}\nState: {state}\nZip Code: {zip_code}\n\n")




  # Need a way to get access to the matching dictionary in the aad_users list of dictionaries based on input from the ADP info
  # i.e.: get passed a user's email address and return the dictionary that contains that user's email address and compare the information we're getting
  # from ADP, pass if it's the same and make the changes with Graph if it is not. 

  # See here: https://stackoverflow.com/questions/7079241/python-get-a-dict-from-a-list-based-on-something-inside-the-dict
  # And here: https://stackoverflow.com/questions/8653516/python-list-of-dictionaries-search
  #found_dict = next((item for item in ms_graph_pull.aad_users if item['mail'] == email), None)
                                      # This is showing as undefined. 
                                      # Might just try to write the function to just make the changes regardless and write in the 
                                      # check to see if the data matches in MS Graph as a second phase of this thing. 
  #print(found_dict)