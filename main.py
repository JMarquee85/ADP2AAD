# ADP>AAD Integration
# Josh Marcus - Talkiatry
# josh.marcus@talkiatry.com
# Initial Release: 1/25/2023

from adp_aad import *
from msgraphpull import *
import logging
import logging.config
import time
#from azure.storage.blob import BlobServiceClient
# https://pypi.org/project/azure-storage-blob/
#from azure.storage.blob import AppendBlobService
# https://github.com/toddkitta/azure-content/blob/master/articles/storage/storage-python-how-to-use-blob-storage.md

# Or just send the log file daily once run to the container and name it accordingly. 

################# Append Blob Service

#append_blob_service = BlockBlobService(account_name='adpaadlogs', account_key='tI6xgGrwl8eMjwnkUa/LunhcSULmEaZ/Do2JuNz2nu1lFNdNahfBWxCcJ5iJdtQBeIJI5IlPFyw7+AStQ1aTuw==')


################# Old Blob Logging Stuff

#from azure_storage_logging.handlers import BlobStorageRotatingFileHandler#

#mystorageaccountname='adpaadlogs'
#mystorageaccountkey='tI6xgGrwl8eMjwnkUa/LunhcSULmEaZ/Do2JuNz2nu1lFNdNahfBWxCcJ5iJdtQBeIJI5IlPFyw7+AStQ1aTuw=='#

#logger = logging.getLogger('service_logger')
#logger.setLevel(logging.DEBUG)
#log_formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s')
#azure_blob_handler = BlobStorageRotatingFileHandler(filename = 'service.log', 
#                                                    account_name=mystorageaccountname,
#                                                    account_key=mystorageaccountkey,
#                                                    maxBytes= 5,
#                                                    container='service-log')
#azure_blob_handler.setLevel(logging.INFO)
#azure_blob_handler.setFormatter(log_formater)
#logger.addHandler(azure_blob_handler)#

#logger.warning('warning message')


#############################################################################
## Create a logger for the 'azure.storage.blob' SDK
#logger = logging.getLogger('azure.storage.blob')
#logger.setLevel(logging.DEBUG)#

## Configure a console output
#handler = logging.StreamHandler(stream=sys.stdout)
#logger.addHandler(handler)#

## This client will log detailed information about its HTTP sessions, at DEBUG level
#service_client = BlobServiceClient.from_connection_string("your_connection_string", logging_enable=True)#

#connection_string = "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=adpaadlogs;AccountKey=tI6xgGrwl8eMjwnkUa/LunhcSULmEaZ/Do2JuNz2nu1lFNdNahfBWxCcJ5iJdtQBeIJI5IlPFyw7+AStQ1aTuw==;BlobEndpoint=https://adpaadlogs.blob.core.windows.net/;FileEndpoint=https://adpaadlogs.file.core.windows.net/;QueueEndpoint=https://adpaadlogs.queue.core.windows.net/;TableEndpoint=https://adpaadlogs.table.core.windows.net/"#

## Create the Blob Storage Client
## blob_service = BlobServiceClient(account_url="https://adpaadlogs.blob.core.windows.net/", credential=credential)
#service = BlobServiceClient.from_connection_string(conn_str=connection_string)



if __name__ == "__main__":
	# Logging.
	# logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',filename="adp2aad.log", level=logging.INFO)

	# Start execution timer. 
	msgraphpull.logger.info(f"Starting script timer.")
	startTime = time.time()
	 
	# Log that the sync has started. 
	msgraphpull.logger.info("ADP>AAD Sync process started!")

	# Authenticate Microsoft
	msgraphpull.logger.info("Authenticating into MS Graph...")

	# Auth into Microsoft Graph API
	ms_auth_token()

	# Call snowflake_user_pull() and pass ms_graph_pull in as an argument to return a list of dictionaries containing the user information to compare.
	snowflake_user_pull(ms_graph_pull())

	# Send latest log file to Azure Blob Storage Here. 

	executionTime = (time.time() - startTime)
	msgraphpull.logger.info(f"Execution time in seconds: {executionTime}")
	print(f"Execution time in seconds: {executionTime}")
