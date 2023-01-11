# ADP2AAD
 A python script to take information from ADP into AAD by way of Snowflake. 

## Purpose
Information regarding employee status, current manager, job title, department, etc. is kept up to date primarily in ADP in the organization. 

There is currently no mechanism that exists to automatically update the information for a user that is changed on the ADP side to keep the information in Microsoft up to date. 

This integration intends to solve that issue. 

## How it Works

The Python Snowflake connector authenticates into Snowflake to pull the current user information into a pandas dataframe for reference. 

Separately, MS Graph API calls pull the current information that is stored in Microsoft for the users into a list of dictionaries. 

The program then loops through the rows in the ADP dataframe that is pulled from Snowflake and assigns each relevant piece of user information into a variable. That information is then used to determine if the user exists in the Microsoft information pulled from Graph. A few checks take place here, namely if the user is marked Terminated on the ADP side, the program checks to see if the user exists on the MS side. If they do, the program deletes the user. 

If they do exist, a function is run to check if all fields match between the ADP and MS information. If they do, the script moves on. If not, the fields from ADP are stored in an API request and sent to the Microsoft Graph API for update. 