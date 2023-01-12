# This is probably not formatted correctly, but I'm putting the main playbook in here. 

from adp_aad import * 
from msgraphpull import * 



# Authenticate Microsoft
print("Authenticating into MS Graph...")
ms_auth_token()
# Pull the Microsoft user information from msgraphpull
print(f"Now pulling AAD users via Microsoft Graph...")

# Call snowflake_user_pull() 
snowflake_user_pull(ms_graph_pull())
 