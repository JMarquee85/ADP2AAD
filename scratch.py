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