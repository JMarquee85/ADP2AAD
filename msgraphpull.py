# QUERYING MS GRAPH
# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c

# import libraries
import msal
import json
import requests  # remove this once everything below is put into its own script.
import pandas as pd
import logging

# start logging
logging.basicConfig(filename="msgraph.log", level=logging.INFO)

#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

# MS authentication token
def ms_auth_token():

    # Set the access_token variable as global to call it elsewhere for auth.
    global token, headers

    # Create a token data dictionary to use with these requests
    app_id = "8af4e31e-7c65-4ad6-8cf0-3a7d67997947"

    # Enter the details of your AAD app registration
    client_id = "8af4e31e-7c65-4ad6-8cf0-3a7d67997947"
    client_secret = "-yj8Q~~JMWNaPV1hpSl1PvzotPEM5Rz1IUGeqau0"
    authority = "https://login.microsoftonline.com/803a6c90-ec72-4384-b2c0-1a376841a04b"
    scope = ["https://graph.microsoft.com/.default"]

    # Create an MSAL instance providing the client_id, authority and client_credential parameters
    client = msal.ConfidentialClientApplication(
        client_id, authority=authority, client_credential=client_secret
    )

    # First, try to lookup an access token in cache
    token_result = client.acquire_token_silent(scope, account=None)

    # If the token is available in cache, save it to a variable
    if token_result:
        access_token = "Bearer " + token_result["access_token"]
        print("Access token was loaded from cache")

    # If the token is not available in cache, acquire a new one from Azure AD and save it to a variable
    if not token_result:
        token_result = client.acquire_token_for_client(scopes=scope)
        access_token = "Bearer " + token_result["access_token"]
        print("New access token acquired from Azure AD!")

    # print(access_token)
    token = access_token

    # Headers variable creation
    headers = {"Authorization": token}

    return token, headers


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

# The function to auth in to MS Graph and pull the user information.
def ms_graph_pull():

    # Get a token.
    # ms_auth_token()

    # Make aad_users global
    global aad_users

    # url = 'https://graph.microsoft.com/v1.0/users?$top=100'
    url = "https://graph.microsoft.com/v1.0/users?$select=id,displayName,userPrincipalName,manager,mail,jobTitle,Department,usertype"

    graph_result = requests.get(url=url, headers=headers)

    ms_dict = graph_result.json()  # dict

    aad_users = ms_dict[
        "value"
    ]  # list of just the user information. Ends up as a list of dictionaries.

    print(f"Paginating Microsoft Graph output...")

    while "@odata.nextLink" in ms_dict:
        if url is None:
            print("No next link found.")
        else:
            url = ms_dict.get("@odata.nextLink")
            graph_result = requests.get(url=url, headers=headers)
            ms_dict = graph_result.json()
            # print(ms_dict['@odata.nextLink'])
            # Add something here that removes Service Accounts and Vendors. Something like:
            # if department != "Service Account" or "Vendor" or "Shared Mailbox":
            aad_users.append(ms_dict["value"])

    print(aad_users)
    return aad_users


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


def get_ms_id(email):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + email
    requests.get(url=graph_url, headers=headers)

    try:
        # Get a user's MSid.
        get_ms_id = requests.get(url=graph_url, headers=headers)
        user_result = get_ms_id.json()
        user_id = user_result["id"]  # This gets the user's ID.
        # print(user_id)
        return user_id
    except:
        print(f"User email {email} not found in Azure AD!")


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


def get_ms_user(ms_id):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + ms_id
    get_user_info = requests.get(url=graph_url, headers=headers)

    # Get a user's information using their MSid.
    get_user_result = get_user_info.json()
    return get_user_result


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################

## A function to update the users with Graph API commands.
# Will likely pass the ADP information into this and compare it to the Graph information.
def update_user(
    emp_id,
    full_name,
    preferred_name,
    email,
    department,
    current_role,
    start_date,
    term_date,
    is_provider,
    manager,
    manager_email,
    city,
    state,
    zip_code,
):

    try:

        user_ms_id = get_ms_id(email)  # returns user_id

        graph_url = graph_url = "https://graph.microsoft.com/v1.0/users/" + user_ms_id

        # CHANGE CORE USER INFO (json)
        # https://learn.microsoft.com/en-us/graph/api/user-update?view=graph-rest-1.0&tabs=http
        # Add additional information to ExtensionAttributes for later use
        # https://learn.microsoft.com/en-us/graph/extensibility-overview?tabs=http
        print(f"Sending HTTP request to change core information for {email}...")
        update_user_body = f'{{"displayName": "{full_name}", "department":"{department}, "jobTitle": "{current_role}", "city": "{city}", "state": "{state}", "employeeId": "{emp_id}", "givenName": "{full_name}", "employeeHireDate": "{start_date}", "postalCode": "{zip_code}", "extensionAttribute5": "{is_provider}"}}'
        # PATCH request to update user goes here:
        update_user_action = requests.patch(
            url=graph_url, data=update_user_body, headers=headers
        )

        # CHANGE USER MANAGER
        # https://learn.microsoft.com/en-us/graph/api/user-post-manager?view=graph-rest-1.0&tabs=http
        # get manager's object id
        manager_id = get_ms_id(manager_email)
        # Assign manager url
        manager_url = graph_url + "manager/$ref"
        manager_update_body = f'"@odata.id": "{graph_url}{manager_id}"'
        print(f"Sending HTTP request to change manager information for {email}...")
        # PUT request to take the action
        manager_update_action = requests.put(
            url=manager_url, data=manager_update_body, headers=headers
        )

    except Exception as e:
        print("Exception encountered: ")
        print(e.message)
        print(f"User {email} probably not found in Azure AD!")


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


# Run stuff

# ms_graph_pull()
# id_number = get_ms_id("test.user@talkiatry.com")
# print(id_number)
# get_ms_user("41967c91-0239-45e2-b318-7625f6584633")

# Test the above function with Test User data.
ms_auth_token()
update_user(
    "123789621736",
    "Testerson Userstein",
    "Test User",
    "test.user@talkiatry.com",
    "Some Different Department",
    "Head of Testing",
    "2021-01-15",
    "None",
    "0",
    "Josh Marcus",
    "josh.marcus@talkiatry.com",
    "Winchester",
    "KY",
    "40391",
)


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################


#####################################
#####################################
# Test for an existing value in the list of dictionaries (aad_users) output
####################################
####################################
####################################

# email = "josh.marcus@talkiatry.com"
#
## https://stackoverflow.com/questions/3897499/check-if-value-already-exists-within-list-of-dictionaries
## list indices must be integers or slices, not str.
# if any(x["userPrincipalName"] == email for x in aad_users):
# print(f"The user {email} is here.")
# else:
# print(f"Sorry, not seeing {email} here...")

###################################
###################################
###################################
