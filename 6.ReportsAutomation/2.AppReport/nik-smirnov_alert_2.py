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
    'start_date': datetime(2023, 8, 18),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_nsmirnov_app_report():

    @task()
    def extract_feed():
        query = '''
                select 
                    toDate(time) as date
                    ,count(distinct user_id) as DAU_feed
                    ,countIf(action = 'view') as views
                    ,countIf(action = 'like') as likes
                    ,count(user_id) as actions
                    ,likes/views * 100 as CTR
                FROM {db}.feed_actions
                WHERE 
                    toDate(time) >= yesterday()-6 and toDate(time) < today()
                GROUP BY 
                    toDate(time)
                    '''
        feed = ph.read_clickhouse(query, connection=read_connection)
        return feed

    @task()
    def extract_messages():
        query = '''
                SELECT 
                    toDate(time) as date
                    ,count(distinct user_id) as DAU_messages
                    ,count(user_id) as messages
                FROM {db}.message_actions
                WHERE 
                    toDate(time) >= yesterday()-6 and toDate(time) < today()
                GROUP BY 
                    toDate(time)
                    '''
        messenger = ph.read_clickhouse(query, connection=read_connection)
        return messenger

    @task()
    def extract_dau():
        query = '''
                SELECT
                    date
                    ,uniqExact(user_id) as DAU
                    ,uniqExactIf(user_id, os = 'iOS') as DAU_ios
                    ,uniqExactIf(user_id, os = 'Android') as DAU_android
                FROM
                    (SELECT  DISTINCT
                        toDate(time) as date
                        ,user_id
                        ,os
                    FROM {db}.feed_actions
                    WHERE toDate(time) >= yesterday()-6 and toDate(time) < today()

                    UNION ALL

                    SELECT  DISTINCT
                        toDate(time) as date
                        ,user_id
                        ,os
                    FROM {db}.message_actions
                    WHERE toDate(time) >= yesterday()-6 and toDate(time) < today()) as t
                GROUP BY 
                    date
                ORDER BY 
                    date   
                    '''
        dau_tbl = ph.read_clickhouse(query, connection=read_connection)
        return dau_tbl

    @task()
    def extract_new_users():
        query = '''
                SELECT 
                    date
                    ,uniqExact(user_id) as new_users
                    ,uniqExactIf(user_id, source = 'ads') as new_users_ads
                    ,uniqExactIf(user_id, source = 'organic') as new_users_organic
                FROM
                    (SELECT 
                        MIN(date) as date
                        ,user_id
                        ,source
                    FROM    
                        (SELECT
                            toDate(MIN(time)) as date 
                            ,user_id
                            ,source
                        FROM {db}.feed_actions
                        GROUP BY 
                            user_id
                            ,source

                        UNION ALL

                        SELECT
                            toDate(MIN(time)) as date 
                            ,user_id
                            ,source
                        FROM {db}.message_actions
                        GROUP BY 
                            user_id
                            ,source) as t
                    GROUP BY 
                        user_id
                        ,source) as tt
                WHERE date >= yesterday()-6 and date < today()
                GROUP BY 
                    date
                ORDER BY 
                    date
                    '''
        new_users_tbl = ph.read_clickhouse(query, connection=read_connection)
        return new_users_tbl

    @task
    def create_message(feed, messenger, dau_tbl, new_users_tbl):
        yesterday = date.today() - timedelta(days = 1)
        day = yesterday.strftime('%d-%m-%Y')

        msg = '''📋ОТЧЕТ ПО ПРИЛОЖЕНИЮ ЗА {day}📋
        
Уникальные пользователи: {DAU:,}
    iOS: {DAU_ios:,}
    Android: {DAU_android:,}
Новые пользователи: {new_users:,}
    ads: {new_users_ads:,}
    organic: {new_users_organic:,}
События: {all_actions:,}
    
ЛЕНТА:
    Уникальные пользователи: {DAU_feed:,}
    Просмотры: {views:,} 
    Лайки: {likes:,} 
    CTR: {CTR:.2f}%

МЕССЕНДЖЕР:
    Уникальные пользователи: {DAU_messages:,}
    Сообщения: {messages:,}'''
        
        msg = msg.format(day = day,
                    all_actions = feed[feed.date == yesterday.strftime('%Y-%m-%d')].actions.iloc[0] 
                                 + messenger[messenger.date == yesterday.strftime('%Y-%m-%d')].messages.iloc[0],
                    DAU = dau_tbl[dau_tbl.date == yesterday.strftime('%Y-%m-%d')].DAU.iloc[0],
                    DAU_ios = dau_tbl[dau_tbl.date == yesterday.strftime('%Y-%m-%d')].DAU_ios.iloc[0],
                    DAU_android = dau_tbl[dau_tbl.date == yesterday.strftime('%Y-%m-%d')].DAU_android.iloc[0],
                    new_users = new_users_tbl[new_users_tbl.date == yesterday.strftime('%Y-%m-%d')].new_users.iloc[0],
                    new_users_ads = new_users_tbl[new_users_tbl.date == yesterday.strftime('%Y-%m-%d')].new_users_ads.iloc[0],
                    new_users_organic = new_users_tbl[new_users_tbl.date == yesterday.strftime('%Y-%m-%d')].new_users_organic.iloc[0],
                    DAU_feed = feed[feed.date == yesterday.strftime('%Y-%m-%d')].DAU_feed.iloc[0],
                    views = feed[feed.date == yesterday.strftime('%Y-%m-%d')].views.iloc[0],
                    likes = feed[feed.date == yesterday.strftime('%Y-%m-%d')].likes.iloc[0],
                    CTR = feed[feed.date == yesterday.strftime('%Y-%m-%d')].CTR.iloc[0],
                    DAU_messages = messenger[messenger.date == yesterday.strftime('%Y-%m-%d')].DAU_messages.iloc[0],
                    messages = messenger[messenger.date == yesterday.strftime('%Y-%m-%d')].messages.iloc[0],).replace(',', ' ')
        return msg
    
    @task
    def create_plots(feed, messenger, dau_tbl, new_users_tbl):
        sns.set(style="whitegrid")

        df = feed.merge(messenger, on = 'date').merge(dau_tbl, on = 'date').merge(new_users_tbl, on = 'date')
        df['all_actions'] = df['actions'] + df['messages']
        df['lpu'] = df['likes'] / df['DAU']
        df['mpu'] = df['messages'] / df['DAU']

        yesterday = date.today() - timedelta(days = 1)
        start = min(df.date).strftime('%d-%m-%Y')
        day = yesterday.strftime('%d-%m-%Y')

        plot_objects = []

        # Первый график
        fig, axes = plt.subplots(2, 2, figsize=(20, 10), dpi = 100)
        fig.suptitle(f'Пользователи приложения в период с {start} по {day}', fontweight='bold')

        sns.lineplot(ax = axes[0, 0],
                    data = pd.melt(df[['date', 'DAU', 'DAU_ios', 'DAU_android']], id_vars = 'date', value_vars = ['DAU', 'DAU_ios', 'DAU_android']),
                    x = 'date',
                    y = 'value',
                    hue = 'variable')
        axes[0, 0].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.DAU) + min(df.DAU)/6), 
                    title = 'Уникальные пользователи')
        axes[0, 0].legend(title = None)

        sns.lineplot(ax = axes[0, 1],
                    data = pd.melt(df[['date', 'new_users', 'new_users_ads', 'new_users_organic']], id_vars = 'date', value_vars = ['new_users', 'new_users_ads', 'new_users_organic']),
                    x = 'date',
                    y = 'value',
                    hue = 'variable')
        axes[0, 1].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.new_users) + min(df.new_users)/6), 
                    title = 'Новые пользователи')
        axes[0, 1].legend(title = None)

        sns.lineplot(data = df, x = 'date', y = 'DAU_feed', ax = axes[1, 0])
        axes[1, 0].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.DAU_feed) + min(df.DAU_feed)/6), 
                    title = 'Уникальные пользователи ленты')

        sns.lineplot(data = df, x = 'date', y = 'DAU_messages', ax = axes[1, 1])
        axes[1, 1].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.DAU_messages) + min(df.DAU_messages)/6), 
                    title = 'Уникальные пользователи мессенджера')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'users_plot.png'
        plt.close()

        plot_objects.append(plot_object)

        # Второй график
        fig, axes = plt.subplots(2, 2, figsize=(20, 10), dpi = 100)
        fig.suptitle(f'Пользовательские действия в период с {start} по {day}', fontweight='bold')

        sns.lineplot(data = df, x = 'date', y = 'all_actions', ax = axes[0, 0])
        axes[0, 0].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.all_actions) + min(df.all_actions)/6), 
                    title = 'Все события')

        sns.lineplot(data = df, x = 'date', y = 'messages', ax = axes[0, 1])
        axes[0, 1].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.messages) + min(df.messages)/6), 
                    title = 'Сообщения')

        sns.lineplot(data = df, x = 'date', y = 'views', ax = axes[1, 0])
        axes[1, 0].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.views) + min(df.views)/6), 
                    title = 'Просмотры')

        sns.lineplot(data = df, x = 'date', y = 'likes', ax = axes[1, 1])
        axes[1, 1].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (0, max(df.likes) + min(df.likes)/6), 
                    title = 'Лайки')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'actions_plot.png'
        plt.close()

        plot_objects.append(plot_object)

        # третий график
        fig, axes = plt.subplots(3, figsize=(10, 16), dpi = 100)
        fig.suptitle(f'Метрики-отношения в период с {start} по {day}', fontweight='bold')

        sns.lineplot(data = df, x = 'date', y = 'CTR', ax = axes[0])
        axes[0].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (max([0, min(df.CTR) - 2.5]), max(df.CTR) + 2.5), 
                    title = 'CTR, %')

        sns.lineplot(data = df, x = 'date', y = 'lpu', ax = axes[1])
        axes[1].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (max([0, min(df.lpu) - min(df.lpu)/5]), max(df.lpu) + max(df.lpu)/5), 
                    title = 'Количество лайков на пользователя приложения')

        sns.lineplot(data = df, x = 'date', y = 'mpu', ax = axes[2])
        axes[2].set(xlabel = None, 
                    ylabel = None, 
                    ylim = (max([0, min(df.mpu) - min(df.mpu)/5]), max(df.mpu) + max(df.mpu)/5), 
                    title = 'Количество сообщений на пользователя приложения')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'relations_plot.png'
        plt.close()

        plot_objects.append(plot_object)

        return plot_objects
    
    @task
    def send_messages(msg, plot_objects, chat_id):
        bot.sendMessage(chat_id=chat_id, text=msg)
        for plot_object in plot_objects:
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        
        
    feed = extract_feed()
    messenger = extract_messages()
    dau_tbl = extract_dau()
    new_users_tbl = extract_new_users()
    msg = create_message(feed, messenger, dau_tbl, new_users_tbl)
    plot_objects = create_plots(feed, messenger, dau_tbl, new_users_tbl)
    send_messages(msg, plot_objects, chat_id)
    

dag_nsmirnov_app_report = dag_nsmirnov_app_report()