# ADP>AAD Integration
# Josh Marcus - Talkiatry
# josh.marcus@talkiatry.com
# Initial Release: 1/25/2023

from adp_aad import *
from msgraphpull import *
import logging
import time

if __name__ == "__main__":
	# Logging.
	logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',filename="adp2aad.log", level=logging.INFO)

	# Start execution timer. 
	logging.info(f"Starting script timer.")
	startTime = time.time()
	 
	# Log that the sync has started. 
	logging.info("ADP>AAD Sync process started!")

	# Authenticate Microsoft
	logging.info("Authenticating into MS Graph...")

	# Auth into Microsoft Graph API
	ms_auth_token()

	# Call snowflake_user_pull() and pass ms_graph_pull in as an argument to return a list of dictionaries containing the user information to compare.
	snowflake_user_pull(ms_graph_pull())

	executionTime = (time.time() - startTime)
	logging.info(f"Execution time in seconds: {executionTime}")
	print(f"Execution time in seconds: {executionTime}")
