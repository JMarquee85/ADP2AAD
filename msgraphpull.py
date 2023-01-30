# QUERYING MS GRAPH
# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c

import msal
import json
import requests  
import pandas as pd
import logging

# Logging.
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',filename="adp2aad.log", level=logging.INFO)

#####################################################################################################
######## Find a way to append all ms managers to the dictionary for each user. #############

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
        logging.info("Access token was loaded from cache")

    # If the token is not available in cache, acquire a new one from Azure AD and save it to a variable
    if not token_result:
        token_result = client.acquire_token_for_client(scopes=scope)
        access_token = "Bearer " + token_result["access_token"]
        logging.info("New access token acquired from Azure AD!")

    # logging.info(access_token)
    token = access_token

    # Headers variable creation
    headers = {"Authorization": token}

    return token, headers


#####################################################################################################

# The function to auth in to MS Graph and pull the user information.
def ms_graph_pull():

    # ms_auth_token() function has to be run to authenticate.

    global aad_users

    # MS Graph API URL
    url = "https://graph.microsoft.com/v1.0/users?$select=id,displayName,givenName,surname,userPrincipalName,manager,mail,jobTitle,Department,usertype,accountEnabled,city,state,postalCode,employeeId"

    graph_result = requests.get(url=url, headers=headers, timeout=60)

    ms_dict = graph_result.json()  # dict

    aad_users = ms_dict[
        "value"
    ]  # list of just the user information. A list of dictionaries.

    logging.info(f"Paginating Microsoft Graph output...")

    while "@odata.nextLink" in ms_dict:
        if url is None:
            logging.info("No next link found.")
        else:
            url = ms_dict.get("@odata.nextLink")
            graph_result = requests.get(url=url, headers=headers, timeout=60)
            ms_dict = graph_result.json()
            # logging.info(ms_dict['@odata.nextLink']) #uncomment this to show the link to the next page of the returned Microsoft data.
            user_return = ms_dict["value"]
            for item in user_return:
                if type(item) is dict:
                    if (
                        item["department"] is not None
                        and item["jobTitle"] is not None
                        and ("#EXT#") not in item["userPrincipalName"]
                        and (
                            "Automation"
                            or "Shared"
                            or "Admin Account"
                            # or "TERMED"
                            # or "Termed"
                            # or "termed"
                            or "Test Account"
                            or "Calendar"
                            or "Mailbox"
                            or "Call Queue"
                            # or "NLE"
                            or "Phone"
                            or "Auto Attendant"
                            or "IVR"
                        )
                        not in item["jobTitle"]
                        and ("Vendor" or "Service Account" or "Shared Mailbox")
                        not in item["department"]
                    ):
                        aad_users.append(item)
    # logging.info(aad_users)
    return aad_users


#####################################################################################################


def get_ms_id(email):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + email
    requests.get(url=graph_url, headers=headers, timeout=15)

    try:
        # Get a user's MSid.
        get_ms_id = requests.get(url=graph_url, headers=headers, timeout=15)
        user_result = get_ms_id.json()
        user_id = user_result["id"]  # This gets the user's ID.
        # logging.info(user_id)
        return user_id
    except:
        logging.info(f"User email {email} not found in Azure AD!")


#####################################################################################################


def get_ms_user(ms_id):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + ms_id
    get_user_info = requests.get(url=graph_url, headers=headers, timeout=15)

    # Get a user's information using their MSid.
    get_user_result = get_user_info.json()
    return get_user_result


#####################################################################################################


def does_user_exist(email):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + str(email)
    get_user_status = requests.get(url=graph_url, headers=headers, timeout=15)

    get_user_info_return = get_user_status.json()

    if "error" in get_user_info_return:
        return False


#####################################################################################################


def delete_user(email):

    # Have to be authenticated with ms_auth_token()
    ms_id = get_ms_id(email)
    graph_del_url = "https://graph.microsoft.com/v1.0/users/" + str(ms_id)

    delete_user_action = requests.delete(
        url=graph_del_url,
        headers=headers,
        timeout=30
    )

    delete_user_actiion_status = delete_user_action.json()

    logging.info(f"User {email} has been deleted from AAD!")

    if "error" in delete_user_actiion_status:
        logging.info(f"Error deleting user {email}!")
        logging.info(f"Error deleting user {email}!")


#####################################################################################################

## A function to update the users with Graph API calls.
def update_user(
    emp_id,
    full_name,
    preferred_name,
    first_name,
    last_name,
    email,
    department,
    current_role,
    start_date,
    term_date,
    is_provider,
    manager,
    manager_email,
    #city,
    state,
    #zip_code,
):

    try:

        user_ms_id = get_ms_id(email)  # returns user_id
        # logging.info(user_ms_id)

        graph_url = "https://graph.microsoft.com/v1.0/users/" + user_ms_id
        # logging.info(graph_url)

        headers = {"Authorization": token, "Content-type": "application/json"}

        # CHANGE CORE USER INFO (json)
        # https://learn.microsoft.com/en-us/graph/api/user-update?view=graph-rest-1.0&tabs=http
        # Add additional information to ExtensionAttributes for later use
        # https://learn.microsoft.com/en-us/graph/extensibility-overview?tabs=http
        logging.info(f"{email} HTTP request sent to change core information ...")
        update_user_body = {
            "displayName": preferred_name,
            "department": department,
            "jobTitle": current_role,
            #"city": city,
            "state": state,
            "employeeId": emp_id,
            "givenName": first_name,
            "surname": last_name,
            "employeeHireDate": start_date,
           #"postalCode": zip_code,
            "employeeId": emp_id,
        }
        # put the information into a JSON format
        user_json = json.dumps(update_user_body)

        # PATCH request to update user goes here:
        update_user_action = requests.patch(
            url=graph_url,
            data=user_json,
            headers=headers,
            timeout=30
        )
        # Show the request
        # print(f"Core information status: {update_user_action.status_code}")
        if update_user_action.status_code == 204:
            logging.info(f"{email} core information update successful!")
        else:
            logging.info(
                f"{email} core information not updated! Status Code: {update_user_action.status_code}"
            )

        # Log successful core information update.
        logging.info(f"{email} core information updated in Azure AD!")

    # Error updating core information.
    except:
        logging.info(f"Error updating core information for {email}!")
        logging.info(f"User {email} likely not in Azure AD... ")
        # Add error to logs.
        logging.info(f"Error updating information for {email} in Azure AD!\n")


#####################################################################################################


def update_manager(email, manager, manager_email):

    # Must be authenticated with ms_auth_token() before using this function.

    try:
        user_ms_id = get_ms_id(email)  # returns user_id
        # logging.info(user_ms_id)

        graph_url = "https://graph.microsoft.com/v1.0/users/" + user_ms_id
        # logging.info(graph_url)

        headers = {"Authorization": token, "Content-type": "application/json"}

        # CHANGE USER MANAGER
        # https://learn.microsoft.com/en-us/graph/api/user-post-manager?view=graph-rest-1.0&tabs=http
        # get manager's object id
        manager_id = get_ms_id(manager_email)
        # Assign manager url
        manager_url = (
            "https://graph.microsoft.com/v1.0/users/" + user_ms_id + "/manager/$ref"
        )
        manager_update_body = {
            "@odata.id": "https://graph.microsoft.com/v1.0/users/" + manager_id
        }
        # put this information into a JSON format
        mgr_json = json.dumps(manager_update_body)
        logging.info(f"{email} changing manager to {manager_email} - sending HTTP request...")
        # PUT request to take the action
        manager_update_action = requests.put(
            url=manager_url,
            data=mgr_json,
            headers=headers,
            timeout=30
        )
        # Show the request
        # print(f"Manager Update Status Code: {manager_update_action.status_code}")
        if manager_update_action.status_code == 204:
            logging.info(f"{email} manager update successful!")
        else:
            logging.info(
                f"Error updating manager! Status Code: {manager_update_action.status_code}"
            )

        # Add the change to the log file
        logging.info(f"{email} manager changed to: {manager_email}!")

    except Exception as manager_update_error:
        logging.error(f"Error encountered changing the manager for {email}!")


#####################################################################################################


def get_ms_user_info(email):

    graph_url = (
        "https://graph.microsoft.com/v1.0/users/"
        + email
        + "?$select=employeeId,displayName,givenName,surname,userPrincipalName,jobTitle,Department,manager,city,state,postalCode"
    )
    get_user_info = requests.get(url=graph_url, headers=headers, timeout=30)
    mgr_graph_url = "https://graph.microsoft.com/v1.0/users/" + email + "/manager"
    get_user_manager = requests.get(url=mgr_graph_url, headers=headers, timeout=30)

    try:
        # Get a user's information using their MSid.
        get_user_result = get_user_info.json()
        get_manager_result = get_user_manager.json()
        logging.info(get_user_result)
        logging.info(f"Manager: {get_manager_result['userPrincipalName']}")
    except:
        logging.info(f"User {email} has no manager!\n")


#####################################################################################################


def get_ms_user_manager(email):

    graph_url = f"https://graph.microsoft.com/v1.0/users/{email}?$select=employeeId,displayName,givenName,surname,userPrincipalName,jobTitle,Department,manager,city,state,postalCode"
    get_user_info = requests.get(url=graph_url, headers=headers, timeout=30)
    mgr_graph_url = f"https://graph.microsoft.com/v1.0/users/{email}/manager"
    get_user_manager = requests.get(url=mgr_graph_url, headers=headers, timeout=30)

    try:
        # Get a user's information using their MSid.
        get_user_result = get_user_info.json()
        get_manager_result = get_user_manager.json()
        return get_manager_result["userPrincipalName"]
    except:
        logging.info(f"User {email} has no manager!")


#####################################################################################################


# A function to return the specific user dictionary to match the ADP user:
def return_msuser_dict(email, employee_id, full_name, preferred_name, dict_list):
    dict_info = [
        element
        for element in dict_list
        if element["userPrincipalName"].casefold() == email
        or element["displayName"].casefold() == full_name
        or element["displayName"].casefold() == preferred_name
        or element["employeeId"] == employee_id
    ]
    for x in dict_info:
        return x


#####################################################################################################


# A function to compare the ADP information with the AAD information and not make the changes if they match.
def user_compare(
    email,
    employee_id,
    full_name,
    preferred_name,
    first_name,
    last_name,
    department,
    title,
    manager_email,
    #city,
    state,
    #zip_code,
    msdict,
):

    # Get user's manager
    ms_manager = get_ms_user_manager(email)

    # Need to handle if a user doesn't have a field filled in in ADP. Currently getting KeyErrors if users don't have information listed in ADP/MSG.
    if (
        full_name != msdict["displayName"]
        or first_name != msdict["givenName"]
        or last_name != msdict["surname"]
        or department != msdict["department"]
        #or city != msdict["city"]
        or state != msdict["state"]
       #or zip_code != msdict["postalCode"]
        or employee_id != msdict["employeeId"]
        or manager_email != ms_manager
    ):
        return "update"
    else:
        return "do not update"


#####################################################################################################
#####################################################################################################
####################################### TESTING GROUNDS #############################################
#####################################################################################################
#####################################################################################################

# Run stuff
# ms_auth_token()
# ms_graph_pull()
# logging.info(aad_users)
# id_number = get_ms_id("test.user@talkiatry.com")
# logging.info(id_number)
# get_ms_user("41967c91-0239-45e2-b318-7625f6584633")

# Test the above function with Test User data.
# ms_auth_token()
# does_user_exist("josh.marcus@talkiatry.com")
# get_ms_user_info("josh.marcus@talkiatry.com")
# update_user(
# "12389612897653",
# "Josh Marcus",
# "Josh Marcus",
# "Josh",
# "Marcus",
# "josh.marcus@talkiatry.com",
# "Technology",
# "Manager - IT Services",
# "2021-07-20",
# "None",
# "0",
# "Dharmendra Sant",
# "dharmendra.sant@talkiatry.com",
# "El Paso",
# "Texas",
# "79930",
# )


#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
#####################################################################################################
