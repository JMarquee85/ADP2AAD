import snowflake.connector
import requests
import pandas as pd
import json
import threading
import multiprocessing as mp
import msgraphpull
from msgraphpull import *
import logging
from tqdm import tqdm

# Logging.
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',filename="adp2aad.log", level=logging.INFO)
# logging.basicConfig(filename="info.log", level=logging.INFO)
# logging.basicConfig(filename="error.log", level=logging.ERROR)

def snowflake_user_pull(ms_user_list):

    # Process list to run at the end of the file. 
    processes = []
    process_group_counter = 1

    # Snowflake Options
    USER = "d919f591-ed37-4df3-aa8d-b757d8a5b8f3"
    ACCOUNT = "pra18133"
    REGION = "us-east-1"
    ROLE = "AAD_ROLE"
    DATABASE = "ANALYTICS"
    SCHEMA = "COMMON"
    WAREHOUSE = "AAD_WH"

    # Azure AD options
    AUTH_CLIENT_ID = "be49d25b-e84f-4a5b-9726-c616a16ca4c5"
    AUTH_CLIENT_SECRET = "nO58Q~hqpy93aKskNwKOrA5tV_7z4YJYH.4QzcB5"
    AUTH_GRANT_TYPE = "client_credentials"
    SCOPE_URL = "api://e2b6615d-8867-4196-bf8b-ba61aa2f53fa/.default"
    TOKEN_URL = "https://login.microsoftonline.com/803a6c90-ec72-4384-b2c0-1a376841a04b/oauth2/v2.0/token"
    PAYLOAD = (
        "client_id={clientId}&"
        "client_secret={clientSecret}&"
        "grant_type={grantType}&"
        "scope={scopeUrl}".format(
            clientId=AUTH_CLIENT_ID,
            clientSecret=AUTH_CLIENT_SECRET,
            grantType=AUTH_GRANT_TYPE,
            scopeUrl=SCOPE_URL,
        )
    )

    logging.info("Getting Snowflake JWT token... ")
    response = requests.post(TOKEN_URL, data=PAYLOAD, timeout=90)
    json_data = json.loads(response.text)
    TOKEN = json_data["access_token"]
    logging.info("Token obtained!")

    # Snowflake connection
    logging.info("Connecting to Snowflake... ")
    ctx = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        region=REGION,
        role=ROLE,
        authenticator="oauth",
        token=TOKEN,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )

    # Set up a cursor object.
    cs = ctx.cursor()
    logging.info("Snowflake cursor object created.")

    # Query snowflake for the current employee data and sort by last name.
    sql_query = """
    SELECT * FROM analytics.common.dim_employees
    ORDER by EMPLOYEE_LAST_NAME;
    """

    try:
        # run the query
        cs.execute("USE WAREHOUSE AAD_WH")
        cs.execute(sql_query)
        results = cs.fetch_pandas_all()

        # Setting dataframe display if printed in the terminal.
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", 500)
        pd.set_option("display.width", 1000)

        df = pd.DataFrame(
            results,
            columns=[col[0] for col in cs.description],
        )

        # Loop through the Snowflake dataframe and set variables.
        for row in tqdm(df.itertuples(name="ADPUserList")):
            # Can store this in a separate file later for readability- see variables.py
            employee_id = getattr(row, "EMPLOYEE_ID")
            employee_full_name = getattr(row, "EMPLOYEE_FULL_NAME")
            employee_preferred_name = getattr(row, "EMPLOYEE_PREFERRED_NAME")
            employee_first_name = getattr(row, "EMPLOYEE_DISPLAY_NAME_FIRST")
            employee_last_name = getattr(row, "EMPLOYEE_DISPLAY_NAME_LAST")
            employee_email = getattr(row, "EMPLOYEE_EMAIL")
            employee_department = getattr(row, "EMPLOYEE_DEPARTMENT_OU_NAME_PREFERRED")
            employee_current_role = getattr(row, "EMPLOYEE_CURRENT_ROLE")
            employee_start_date = getattr(row, "EMPLOYEE_CURRENT_START_DATE")
            employee_status = getattr(
                row, "EMPLOYEE_STATUS"
            )  # Active, Terminated, or Inactive (i.e. on leave)
            employee_separation_date = getattr(
                row, "EMPLOYEE_SEPARATION_DATE"
            )  # To filter for termed users, can use if employee_separation_date is not None:
            is_provider = getattr(
                row, "IS_EMPLOYEE_CLINICAL_PROVIDER"
            )  # 1 for yes, 0 for no
            employee_supervisor_name = getattr(row, "EMPLOYEE_SUPERVISOR_NAME")
            employee_supervisor_email = getattr(row, "EMPLOYEE_SUPERVISOR_EMAIL")
            #employee_city = getattr(row, "EMPLOYEE_CITY")
            employee_state = getattr(row, "EMPLOYEE_STATE")
            #employee_zip = getattr(row, "EMPLOYEE_ZIP")

            # Practicing state variable assignments for later use:
            # Will assign these to extension attributes on the MS side for dynamic distros 
            employee_licensed_states = getattr(row, "EMPLOYEE_LICENSED_STATES")
            #if employee_licensed_states:
                #print(f"{employee_preferred_name} licensed states: {employee_licensed_states}\n")
            employee_certifications = getattr(row, "EMPLOYEE_CERTIFICATIONS")
           #if employee_certifications:
                #print(f"{employee_preferred_name} certifications: {employee_certifications}\n")

            # Get the dictionary related to this user in MSGraph
            if employee_email:
                return_dict = return_msuser_dict(
                    employee_email.casefold(),
                    employee_id.casefold(),
                    employee_full_name.casefold(),
                    employee_preferred_name.casefold(),
                    ms_user_list,
                )
                # logging.info(return_dict) # Aaaaand this is a list, not a dictionary.
            elif employee_email is None or "@talkiatry.com" not in employee_email:
                return_dict = return_msuser_dict(
                    employee_email,
                    employee_id,
                    employee_full_name,
                    employee_preferred_name,
                    ms_user_list,
                )
            else:
                return_dict = return_msuser_dict(
                    employee_email,
                    employee_id,
                    employee_full_name,
                    employee_preferred_name,
                    ms_user_list,
                )

            # Check if user is terminated.
            if employee_status == "Terminated":
                # Check if employee exists in MS. If so, deactivate account and delete. If not, move on.
                if return_dict:
                    logging.log(f"{employee_email} marked termed on {employee_separation_date}")
                    logging.info(
                        f"{employee_email} found in the MSGraph dictionaries. Now deleting {employee_email} from Azure AD!\n"
                    )
                    delete_user(employee_email)
                else:
                    logging.info(
                        f"{employee_email} termed on {employee_separation_date}, not found in Azure AD. Should be deleted already.\n"
                    )

            # Action if user is Active
            elif (
                employee_status == "Active" or "Inactive"
            ):  # Inactive covers users on leave and keep their information updated.
                ##### Compare ADP and MS info.
                # Don't compare or change any email addresses. We will handle our own copy of those specific fields.
                if return_dict:
                    compare = user_compare(
                        employee_email,
                        employee_id,
                        employee_full_name,
                        employee_preferred_name,
                        employee_first_name,
                        employee_last_name,
                        employee_department,
                        employee_current_role,
                        employee_supervisor_email,
                        #employee_city,
                        employee_state,

                        #employee_zip,
                        return_dict,
                    )
                    # If the result of user_compare() detects differences in the core information and what is listed in ADP
                    # Make this more specific in the future, like only change the manager if it's incorrect or change the city and state if they moved. This might require some kind of returns from the function to flag what needs to be changed.

                    if compare == "update":
                        update_process = mp.Process(
                            target=update_user,
                            args=(
                                employee_id,
                                employee_full_name,
                                employee_preferred_name,
                                employee_first_name,
                                employee_last_name,
                                employee_email,
                                employee_department,
                                employee_current_role,
                                employee_start_date,
                                employee_separation_date,
                                is_provider,
                                employee_supervisor_name,
                                employee_supervisor_email,
                                #employee_city,
                                employee_state,
                                #employee_zip,
                            )
                        )
                        update_process.start()
                        processes.append(update_process)
                        
                        # Write new function called check_mgr and return update if it should be changed.
                        # logging.info(f"Updating manager for {employee_full_name}...")

                        manager_process = mp.Process(target=update_manager,args=(employee_email,
                            employee_supervisor_name,
                            employee_supervisor_email))
                        manager_process.start()
                        processes.append(manager_process)

                    else:
                        logging.info(
                            f"All fields stored in ADP match AAD for {employee_email}! Not making any changes..."
                        )
                else:
                    logging.info(
                        f"\nNo MS graph dictionary found for {employee_email}!\nStart Date: {employee_start_date}\n(Have they been created in AAD yet? Have we sent the right information to compare?)\n"
                    )
                    no_graph_output = f"Email: {employee_email}\nName: {employee_full_name}\nPreferred Name: {employee_preferred_name}\nFirst Name: {employee_first_name}\nLast Name: {employee_last_name}\nDepartment: {employee_department}\nTitle: {employee_current_role}\nManager: {employee_supervisor_name}\nState: {employee_state}\nHire Date: {employee_start_date}\nADP Employment Status: {employee_status}\n\n"
                    logging.info(no_graph_output)

            # Action if neither is true.
            else:
                logging.info(f"ADP employee_status not found for {employee_email}!\n")

            # Run the joined processes stored in the processes list when they reach a certain number. 
            if len(processes) >= 50:
                logging.info(f"Running process group {process_group_counter}...")
                for process in tqdm(processes, desc=(f'Running process group #{process_group_counter}')):
                    logging.info(f"Running process {process}..")
                    process.join()
                processes = []
                process_group_counter += 1
            else:
                continue 

    finally:
        for process in tqdm(processes, desc=(f'Running process group #{process_group_counter}')):
            logging.info(f"Running process {process}..")
            process.join()
        cs.close()
        logging.info(f"Snowflake session closed.")
        logging.info(f"ADP>AAD Sync Complete!")
