# import
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import logging
import pandas as pd
import pandahouse as ph
import os
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task


bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
chat_id = 926157354 #-927780322

# Cоединения с базой
read_connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230720',
                      'user':'student', 
                      'password':os.environ.get("PASSWORD")
                     }


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'nik.smirnov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 17),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_nsmirnov_feed_report():

    @task()
    def extract_feed():
        query = '''
                select 
                    toDate(time) as date
                    ,count(distinct user_id) as DAU
                    ,countIf(action = 'view') as views
                    ,countIf(action = 'like') as likes
                    ,likes/views * 100 as CTR
                FROM {db}.feed_actions
                WHERE 
                    toDate(time) >= yesterday()-6 and toDate(time) < today()
                GROUP BY 
                    toDate(time)
                    '''
        df = ph.read_clickhouse(query, connection=read_connection)
        return df


    @task
    def create_message(df):
        yesterday = date.today() - timedelta(days = 1)
        start = min(df.date).strftime('%d-%m-%Y')
        day = yesterday.strftime('%d-%m-%Y')
    
    
        DAU = 0 if df[df.date == yesterday.strftime('%Y-%m-%d')].DAU.empty else df[df.date == yesterday.strftime('%Y-%m-%d')].DAU.iloc[0]
        views = 0 if df[df.date == yesterday.strftime('%Y-%m-%d')].views.empty else df[df.date == yesterday.strftime('%Y-%m-%d')].views.iloc[0]
        likes = 0 if df[df.date == yesterday.strftime('%Y-%m-%d')].likes.empty else df[df.date == yesterday.strftime('%Y-%m-%d')].likes.iloc[0]
        CTR = 0 if df[df.date == yesterday.strftime('%Y-%m-%d')].CTR.empty else round(df[df.date == yesterday.strftime('%Y-%m-%d')].CTR.iloc[0], 2)
        msg = 'Отчет по ленте новостей за {}: \n\n1) DAU = {:,} \n2) Просмотры = {:,} \n3) Лайки = {:,} \n4) CTR = {}%'\
            .format(day, DAU, views, likes, CTR).replace(',', ' ')
        msg = msg + '\n\nПодробности: http://superset.lab.karpov.courses/r/4260'
        return msg

    @task
    def create_plot(df):
        sns.set(style="whitegrid")
        
        yesterday = date.today() - timedelta(days = 1)
        start = min(df.date).strftime('%d-%m-%Y')
        day = yesterday.strftime('%d-%m-%Y')
        
        fig = plt.figure(figsize=(3 * 6, 3 * 3), dpi = 100) 
        gs = fig.add_gridspec(2, 2)
        fig.suptitle(f'Метрики ленты за период с {start} по {day}', fontsize=14, fontweight='bold')

        ax_dau = fig.add_subplot(gs[0, 0])
        ax_views = fig.add_subplot(gs[1, 1])
        ax_likes = fig.add_subplot(gs[1, 0]) 
        ax_ctr = fig.add_subplot(gs[0, 1]) 

        dau = sns.lineplot(x = df.date, y = df.DAU, ax = ax_dau)
        dau.set(xlabel=None, ylabel = None, ylim = (0, max(df.DAU) + min(df.DAU)/5))
        dau.set_title('DAU')

        views = sns.lineplot(x = df.date, y = df.views, ax = ax_views)
        views.set(xlabel=None, ylabel = None, ylim = (0, max(df.views) + min(df.views)/5))
        views.set_title('Просмотры')

        likes = sns.lineplot(x = df.date, y = df.likes, ax = ax_likes)
        likes.set(xlabel=None, ylabel = None, ylim = (0, max(df.likes) + min(df.likes)/5))
        likes.set_title('Лайки')

        ctr = sns.lineplot(x = df.date, y = df.CTR, ax = ax_ctr)
        ctr.set(xlabel=None, ylabel = None, ylim = (max([0, min(df.CTR) - 2.5]), max(df.CTR) + 2.5))
        ctr.set_title('CTR')


        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plot.png'
        plt.close()
        
        return plot_object
    
    @task
    def send_messages(msg, plot_object, chat_id):
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        
        
    df = extract_feed()
    msg = create_message(df)
    plot_object = create_plot(df)
    send_messages(msg, plot_object, chat_id)
    

dag_nsmirnov_feed_report = dag_nsmirnov_feed_report()