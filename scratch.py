###############################
# Some useful sample SQL queries:
# Select Legal Formatted nam (like Marcus, Joshua )
cs.execute("show tables like 'PERSON_LEGAL_FORMATTED_NAME' in schema analytics.source;")
# First Name
cs.execute("show tables like 'PERSON_LEGAL_GIVEN_NAME' in schema analytics.source;")
# Last Name
cs.execute("show tables like 'PERSON_LEGAL_FAMILY_NAME_1 ' in schema analytics.source;")

# Can plug in any of the following models to get information like this:
#   Column                                            Non-Null Count  Dtype                              
---  ------                                            --------------  -----                              
 0   PERSON_WORKER_ID                                  2874 non-null   object                             
 1   PERSON_LEGAL_FORMATTED_NAME                       2874 non-null   object                             
 2   PERSON_LEGAL_GIVEN_NAME                           2874 non-null   object                             
 3   PERSON_LEGAL_MIDDLE_NAME                          1135 non-null   object                             
 4   PERSON_LEGAL_FAMILY_NAME_1                        2874 non-null   object                             
 5   PERSON_LEGAL_FAMILY_NAME_2                        0 non-null      object                             
 6   PERSON_LEGAL_NICK_NAME                            237 non-null    object                             
 7   PERSON_NPI                                        788 non-null    object                             
 8   PERSON_LEGAL_CERTIFICATION_TITLE                  180 non-null    object                             
 9   PERSON_LEGAL_GENERATION_SUFFIX                    12 non-null     object                             
 10  PERSON_PREFERRED_FAMILY_NAME_1                    131 non-null    object                             
 11  PERSON_PREFERRED_FAMILY_NAME_2                    0 non-null      object                             
 12  PERSON_PREFERRED_GIVEN_NAME                       237 non-null    object                             
 13  PERSON_PREFERRED_MIDDLE_NAME                      14 non-null     object                             
 14  PERSON_LEGAL_ADDRESS_CITY_NAME                    2873 non-null   object                             
 15  PERSON_LEGAL_ADDRESS_COUNTRY_SUBDIVISION_LEVEL_1  2873 non-null   object                             
 16  PERSON_LEGAL_ADDRESS_COUNTRY_CODE                 2873 non-null   object                             
 17  PERSON_LEGAL_ADDRESS_POSTAL_CODE                  2873 non-null   object                             
 18  PERSON_BIRTH_EPOCH                                2874 non-null   object                             
 19  PERSON_TIMESTAMP                                  2874 non-null   datetime64[ns, America/Los_Angeles]
 20  MODEL_GENERATED_AT                                2874 non-null   datetime64[ns, America/Los_Angeles]

#### Things I need:

Date of hire
cs.execute("show tables like 'WORKER_ORIGINAL_HIRE_DATE' in schema analytics.source;")
Employment Status
cs.execute("show tables like 'WORKER_TERMINATION_DATE' in schema analytics.source;")
# if termination date not null... 
Termination Date
cs.execute("show tables like '' in schema analytics.source;")
# These look to be available in ANALYTICS.SOURCE.SRC_ADP_WORKERS
SELECT * FROM analytics.source.src_adp_workers;
# Not certain how to match this to the worker number from the other tables. Look for some kind of statement to match it with the item in an array and if it's a match, append the status to the matching line in the array. 



# Cut queries:

sql_query = "show tables like '%ADP%' in schema analytics.staging;"
sql_query2 = "show tables like '%ADP%' in schema analytics.staging;"
sql_query3 = '''
SELECT *

FROM

    analytics.source.src_adp_persons

WHERE

    person_worker_id IN ( '9V0WOIFLL' );

--	QUALIFY

--		ROW_NUMBER( ) OVER 

--			(PARTITION BY person_worker_id 

--			ORDER BY person_timestamp DESC ) = 1;
'''

sql_query4 = '''
'''



cs.execute(sql_query)
cs.execute(sql_query2)
cs.execute(sql_query3)
cs.execute(sql_query4)



