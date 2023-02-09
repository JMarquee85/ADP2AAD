# QUERYING MS GRAPH
# Created AAD Enterprise Application called ADP-AAD Graph
# https://towardsdatascience.com/querying-microsoft-graph-api-with-python-269118e8180c

import msal
import json
import requests  
import datetime
from requests.sessions import HTTPAdapter
from requests.adapters import Retry
import pandas as pd
#import adp_aad
#from adp_aad import *
#from adp_aad import adp2aad_logs
import logging
import logging.config
from os import path
import os
import glob
from azure.storage.blob import BlobServiceClient

# Too many concurrent requests if run back to back. JSON batching.
# https://learn.microsoft.com/en-us/graph/json-batching     

#################################################################################################
# Logging
def logging_setup():
    logging.config.fileConfig('log_config.conf')
    logger = logging.getLogger('MainLogger')
    fh = logging.FileHandler(path.join(path.dirname(path.abspath(__file__)), 'logs/{:%Y-%m-%d_%H:%M:%S}.log'.format(datetime.datetime.now())))
    print(log_filename)
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

##################################################################################################
# Azure Storage Account Variables for Log Storage
storage_account_key = 'tI6xgGrwl8eMjwnkUa/LunhcSULmEaZ/Do2JuNz2nu1lFNdNahfBWxCcJ5iJdtQBeIJI5IlPFyw7+AStQ1aTuw=='
storage_account_name = 'adpaadlogs'
container_name = 'adp2aadlogs'
connection_string = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=adpaadlogs;AccountKey=tI6xgGrwl8eMjwnkUa/LunhcSULmEaZ/Do2JuNz2nu1lFNdNahfBWxCcJ5iJdtQBeIJI5IlPFyw7+AStQ1aTuw==;BlobEndpoint=https://adpaadlogs.blob.core.windows.net/;FileEndpoint=https://adpaadlogs.file.core.windows.net/;QueueEndpoint=https://adpaadlogs.queue.core.windows.net/;TableEndpoint=https://adpaadlogs.table.core.windows.net/"

def send_logs():
    # locates latest log file and sends to Azure Storage account. 
    log_files = glob.glob('logs/*')
    new_log = max(log_files, key=os.path.getctime)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=new_log)

    with open(new_log,"rb") as data:
        blob_client.upload_blob(data)
    logger.info(f"Uploaded {new_log} to Azure Storage Account.")

##################################################################################################
# A requests session configured with retries.

def create_http_session():

    http_ = requests.Session()
    
    # Retry has been set for all server related errors
    retry_ = Retry(total=15, backoff_factor=5, status_forcelist=[500, 502, 503, 504])
    adaptor = HTTPAdapter(max_retries=retry_)
    http_.mount('https://', adaptor)

    return http_

http_session = create_http_session()

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
        logger.info("Access token was loaded from cache")

    # If the token is not available in cache, acquire a new one from Azure AD and save it to a variable
    if not token_result:
        token_result = client.acquire_token_for_client(scopes=scope)
        access_token = "Bearer " + token_result["access_token"]
        logger.info("New access token acquired from Azure AD!")

    # logger.info(access_token)
    token = access_token

    # Headers variable creation
    headers = {"Authorization": token}

    #print(token)
    #print(headers)
    return token, headers


#####################################################################################################

# The function to auth in to MS Graph and pull the user information.


def ms_graph_pull():

    # ms_auth_token() function has to be run to authenticate.

    global aad_users

    # MS Graph API URL
    url = "https://graph.microsoft.com/v1.0/users?$select=id,displayName,givenName,surname,userPrincipalName,manager,jobTitle,Department,usertype,accountEnabled,state,employeeId"

    graph_result = http_session.get(url=url, headers=headers, timeout=30)

    ms_dict = graph_result.json()  # dict

    aad_users = ms_dict[
        "value"
    ]  # list of just the user information. A list of dictionaries.

    logger.info(f"Paginating Microsoft Graph output...")

    while "@odata.nextLink" in ms_dict:
        if url is None:
            logger.info("No next link found.")
        else:
            url = ms_dict.get("@odata.nextLink")
            graph_result = http_session.get(url=url, headers=headers, timeout=30)
            ms_dict = graph_result.json()
            #logger.info(ms_dict['@odata.nextLink']) #uncomment this to show the link to the next page of the returned Microsoft data.
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
    return aad_users


#####################################################################################################


def get_ms_id(email):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + email
    http_session.get(url=graph_url, headers=headers)

    try:
        # Get a user's MSid.
        get_ms_id = http_session.get(url=graph_url, headers=headers, timeout=30)
        user_result = get_ms_id.json()
        user_id = user_result["id"]  # This gets the user's ID.
        # print(user_id)
        return user_id
    except:
        logger.info(f"{email} - User email not found in Azure AD!")


#####################################################################################################


def get_ms_user(ms_id):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + ms_id
    get_user_info = http_session.get(url=graph_url, headers=headers, timeout=30)

    # Get a user's information using their MSid.
    get_user_result = get_user_info.json()
    return get_user_result


#####################################################################################################


def does_user_exist(email):

    graph_url = "https://graph.microsoft.com/v1.0/users/" + str(email)
    get_user_status = http_session.get(url=graph_url, headers=headers, timeout=30)

    get_user_info_return = get_user_status.json()

    if "error" in get_user_info_return:
        return False


#####################################################################################################


def delete_user(email):

    # Have to be authenticated with ms_auth_token()
    ms_id = get_ms_id(email)
    graph_del_url = "https://graph.microsoft.com/v1.0/users/" + str(ms_id)

    delete_user_action = http_session.delete(
        url=graph_del_url,
        headers=headers, 
        timeout=30
    )

    delete_user_actiion_status = delete_user_action.json()

    logger.info(f"{email} - User has been deleted from AAD!")

    if "error" in delete_user_actiion_status:
        logger.error(f"{email} - Error deleting user!")

#####################################################################################################

## A function to update the users with Graph API calls.
def update_user(
    token,
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
        # logger.info(user_ms_id)

        graph_url = "https://graph.microsoft.com/v1.0/users/" + user_ms_id
        # logger.info(graph_url)

        headers = {"Authorization": token, "Content-type": "application/json"}

        # CHANGE CORE USER INFO (json)
        # https://learn.microsoft.com/en-us/graph/api/user-update?view=graph-rest-1.0&tabs=http
        # Add additional information to ExtensionAttributes for later use
        # https://learn.microsoft.com/en-us/graph/extensibility-overview?tabs=http
        logger.info(f"{email} HTTP request sent to change core information ...")
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
        update_user_action = http_session.patch(
            url=graph_url,
            data=user_json,
            headers=headers,
            timeout=30
        )
        # Show the request
        #print(f"Core information status: {update_user_action.status_code}")
        if update_user_action.status_code == 204:
            logger.info(f"{email} core information update successful!")
        else:
            logger.info(
                f"{email} core information not updated! Status Code: {update_user_action.status_code}"
            )

        # Log successful core information update.
        logger.info(f"{email} core information updated in Azure AD!")

    # Error updating core information.
    except:
        logger.error(f"{email} - Error updating core information!")
        logger.error(f"{email} - Email does not appear to exist in Azure AD... ")
        # Add error to logs.
        logger.error(f"{email} - Error updating information in Azure AD!")


#####################################################################################################


def update_manager(email, manager, manager_email):

    # Must be authenticated with ms_auth_token() before using this function.

    try:

        # Not getting manager ID. Seems to be returning as Nonetype. 

        user_ms_id = get_ms_id(email)  # returns user_id
        logger.info(f"User ID: {user_ms_id}")

        graph_url = "https://graph.microsoft.com/v1.0/users/" + user_ms_id
        logger.info(graph_url)

        headers = {"Authorization": token, "Content-type": "application/json"}

        # CHANGE USER MANAGER
        # https://learn.microsoft.com/en-us/graph/api/user-post-manager?view=graph-rest-1.0&tabs=http
        # get manager's object id
        manager_id = get_ms_id(manager_email)
        logger.info(f"Manager ID: {manager_id}")
        # Assign manager url
        manager_url = (
            "https://graph.microsoft.com/v1.0/users/" + user_ms_id + "/manager/$ref"
        )
        manager_update_body = {
            "@odata.id": "https://graph.microsoft.com/v1.0/users/" + manager_id
        }
        # put this information into a JSON format
        mgr_json = json.dumps(manager_update_body)
        logger.info(f"{email} changing manager to {manager_email} - sending HTTP request...")
        # PUT request to take the action
        manager_update_action = http_session.put(
            url=manager_url,
            data=mgr_json,
            headers=headers,
            timeout=30
        )
        # Show the request
        #print(f"Manager Update Status Code: {manager_update_action.status_code}")
        if manager_update_action.status_code == 204:
            logger.info(f"{email} - manager update successful!")
        else:
            logger.error(
                f"{email} - error updating manager! Status Code: {manager_update_action.status_code}"
            )

        # Add the change to the log file
        logger.info(f"{email} - manager changed to: {manager_email}!")

    except Exception as e:
        logger.error(f"{email} - error encountered changing user manager: {str(e)}")


#####################################################################################################


def get_ms_user_info(email):

    graph_url = (
        "https://graph.microsoft.com/v1.0/users/"
        + email
        + "?$select=employeeId,displayName,givenName,surname,userPrincipalName,jobTitle,Department,manager,city,state,postalCode"
    )
    get_user_info = http_session.get(url=graph_url, headers=headers, timeout=30)
    mgr_graph_url = "https://graph.microsoft.com/v1.0/users/" + email + "/manager"
    get_user_manager = http_session.get(url=mgr_graph_url, headers=headers, timeout=30)

    try:
        # Get a user's information using their MSid.
        get_user_result = get_user_info.json()
        get_manager_result = get_user_manager.json()
        print(get_user_result)
        print(f"Manager: {get_manager_result['userPrincipalName']}")
    except:
        print(f"User {email} has no manager!\n")


#####################################################################################################


def get_ms_user_manager(email):

    graph_url = f"https://graph.microsoft.com/v1.0/users/{email}?$select=employeeId,displayName,givenName,surname,userPrincipalName,jobTitle,Department,manager,city,state,postalCode"
    get_user_info = http_session.get(url=graph_url, headers=headers, timeout=30)
    mgr_graph_url = f"https://graph.microsoft.com/v1.0/users/{email}/manager"
    get_user_manager = http_session.get(url=mgr_graph_url, headers=headers, timeout=30)

    try:
        # Get a user's information using their MSid.
        get_user_result = get_user_info.json()
        get_manager_result = get_user_manager.json()
        return get_manager_result["userPrincipalName"]
    except:
        logger.info(f"{email} - user has no manager!")


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

########################################################