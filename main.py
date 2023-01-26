# ADP>AAD Integration
# Josh Marcus - Talkiatry
# josh.marcus@talkiatry.com
# Initial Release: 1/25/2023

from adp_aad import *
from msgraphpull import *

# Authenticate Microsoft
print("Authenticating into MS Graph...")

# Auth into Microsoft Graph API
ms_auth_token()

# Call snowflake_user_pull() and pass ms_graph_pull in as an argument to return a list of dictionaries containing the user information to compare.
snowflake_user_pull(ms_graph_pull())
