# ADP>AAD Integration
# Josh Marcus - Talkiatry
# josh.marcus@talkiatry.com
# Initial Release: 1/25/2023

from adp_aad import *
from msgraphpull import *
import msgraphpull
import logging
import logging.config
import time

if __name__ == "__main__":
	# Start logging processes.
	msgraphpull.logging_setup()

	# Start execution timer. 
	msgraphpull.logger.info(f"Starting script timer.")
	startTime = time.time()
	 
	# Log that the sync has started. 
	msgraphpull.logger.info("ADP>AAD Sync process started!")

	# Authenticate Microsoft
	msgraphpull.logger.info("Authenticating into MS Graph...")

	#print(msgraphpull.log_filename)

	# Auth into Microsoft Graph API
	ms_auth_token()

	# Call snowflake_user_pull() and pass ms_graph_pull in as an argument to return a list of dictionaries containing the user information to compare.
	snowflake_user_pull(ms_graph_pull())

	# Send latest log file to Azure Blob Storage Here. 
	msgraphpull.send_logs()

	executionTime = (time.time() - startTime)
	msgraphpull.logger.info(f"Execution time in seconds: {executionTime}")
	print(f"Execution time in seconds: {executionTime}")
