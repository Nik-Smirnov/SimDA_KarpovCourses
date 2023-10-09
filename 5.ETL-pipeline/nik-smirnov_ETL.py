# import
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import os

from airflow.decorators import dag, task


# Cоединения с базой
read_connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230720',
                      'user':'student', 
                      'password':os.environ.get("PASSWORD")
                     }

write_connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':os.environ.get("WRITE_PASSWORD")
                     }


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'nik.smirnov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 10),
}

# Интервал запуска DAG
schedule_interval = '@daily'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_nsmirnov_etl():

    @task()
    def extract_feed():
        query_feed = '''
            SELECT
                toDate(time) as event_date
                ,user_id
                ,countIf(action = 'view') as views
                ,countIf(action = 'like') as likes
            FROM {db}.feed_actions
            WHERE  
                toDate(time) = yesterday()
            GROUP BY 
                toDate(time)
                ,user_id
            '''
        df_feed = ph.read_clickhouse(query_feed, connection=read_connection)
        return df_feed
    
    @task()
    def extract_message():
        query_messages = '''       
            SELECT
                yesterday() as event_date
                ,if(user_id = 0, reciever_id, user_id) as user_id
                ,messages_received
                ,messages_sent
                ,users_received
                ,users_sent
            FROM    
                (SELECT
                    user_id
                    ,count(reciever_id) as messages_sent
                    ,count(distinct reciever_id) as users_sent
                FROM {db}.message_actions
                WHERE  
                    toDate(time) = yesterday()
                GROUP BY 
                    user_id
                    ) as t1
                FULL JOIN
                (SELECT 
                    reciever_id
                    ,COUNT(user_id) as messages_received
                    ,COUNT(DISTINCT user_id) as users_received
                FROM {db}.message_actions
                WHERE  
                    toDate(time) = yesterday()
                GROUP BY 
                   reciever_id) as t2
                ON t1.user_id = t2.reciever_id
            '''
        df_message = ph.read_clickhouse(query_messages, connection=read_connection)
        return df_message
    
    @task()
    def extract_user_attributes():
        query_users = '''
                SELECT distinct
                    user_id
                    ,os
                    ,gender
                    ,age
                FROM    
                    (SELECT 
                        user_id
                        ,os
                        ,gender
                        ,age
                    FROM {db}.message_actions
                    UNION ALL
                    SELECT
                        user_id
                        ,os
                        ,gender
                        ,age
                    FROM {db}.feed_actions) t
                '''
        df_users = ph.read_clickhouse(query_users, connection=read_connection)
        
        #без этой таблицы можем потерять (или оставить без ос, пола и возраста) пользователей, которые в конкретный день получали сообщения, 
        #но сами ничего не отправляли и не сидели в ленте (например, user_id 120851 9 августа 2023)
        
        return df_users

    @task
    def transform_unite(df_feed, df_message, df_users):
        df_united = df_feed.merge(df_message, how = 'outer', on = ['event_date','user_id']).fillna(0).merge(df_users, on = 'user_id')
        return df_united

    @task
    def transform_os(df_united):
        cube_os = df_united.groupby(by = ['event_date', 'os'], as_index = False).agg({'views':'sum',
                                                                'likes':'sum',
                                                                'messages_received':'sum',
                                                                'messages_sent':'sum',
                                                                'users_received':'sum',
                                                                'users_sent':'sum'}).\
                                                                rename(columns = {'os' : 'dimension_value'})
        cube_os['dimension'] = 'os'
        return cube_os

    @task
    def transform_gender(df_united):
        cube_gender = df_united.groupby(by = ['event_date', 'gender'], as_index = False).agg({'views':'sum',
                                                                'likes':'sum',
                                                                'messages_received':'sum',
                                                                'messages_sent':'sum',
                                                                'users_received':'sum',
                                                                'users_sent':'sum'}).\
                                                                rename(columns = {'gender' : 'dimension_value'})
        cube_gender['dimension'] = 'gender'
        return cube_gender
   
    @task
    def transform_age(df_united):
        cube_age = df_united.groupby(by = ['event_date', 'age'], as_index = False).agg({'views':'sum',
                                                                'likes':'sum',
                                                                'messages_received':'sum',
                                                                'messages_sent':'sum',
                                                                'users_received':'sum',
                                                                'users_sent':'sum'}).\
                                                                rename(columns = {'age' : 'dimension_value'})
        cube_age['dimension'] = 'age'
        return cube_age
    
    @task
    def transform_concat(cube_os, cube_gender, cube_age):
        final_cube = pd.concat([cube_os, cube_gender, cube_age])
        final_cube = final_cube[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        final_cube = final_cube.astype({'views':'int64',
                    'likes':'int64',
                    'messages_received':'int64',
                    'messages_sent':'int64',
                    'users_received':'int64',
                    'users_sent':'int64'})
        return final_cube
        
    @task
    def load(final_cube):
        query = '''
        CREATE TABLE IF NOT EXISTS test.nsmirnov_ETL_20230720
            (event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_received Int64,
            messages_sent Int64,
            users_received Int64,
            users_sent Int64
            )
            ENGINE = MergeTree()
            ORDER BY event_date
            '''
        ph.execute(query = query, connection = write_connection)
        ph.to_clickhouse(df=final_cube, table='nsmirnov_ETL_20230720', index=False, connection=write_connection)

    df_feed = extract_feed()
    df_message = extract_message()
    df_users = extract_user_attributes()
    df_united = transform_unite(df_feed, df_message, df_users)
    cube_os = transform_os(df_united)
    cube_gender = transform_gender(df_united)
    cube_age = transform_age(df_united)
    final_cube = transform_concat(cube_os, cube_gender, cube_age)
    load(final_cube)

dag_nsmirnov_etl = dag_nsmirnov_etl()
