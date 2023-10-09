# import
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
import io
import sys
import os
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'nik.smirnov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 26),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

# Функция поиска аномалий
def check_anomaly(df, metric, a = 1.5, days = 4):
    """ Функция check_anomaly предлагает алгоритм проверки значения на аномальность с помощью межквартильного размаха. 
    Для расчета квартилей в конкретный пятнадцатиминутный интервал алгоритм берет статитику за несколько предущих дней (аргумент days), а именно:
    значения метрики за такой же пятнадцатиминутный интервал, а также за 2 предшествующих и 2 последующих ему.
    
    Параметры:
    df - датафрейм;
    metric - проверяемая метрика (название столбца в df);
    a − коэффициент, определяющий ширину доверительного интервала;
    days - число дней, статистика по которым берется для определения межквартильного размаха. По умолчанию указали 4, потому что на момент разработки системы алертов 7-дневный период дает слишком широкие доверительные интервалы для лайков и просмотров. Это связано с тем, что по сравнению с данными примерно недельной давности (и несколькими днями после) эти метрики сильно выросли. Если метрики перестанут расти от недели к неделе, то значение этого параметра можно будет установить раным 7.
    
    Функция возвращает: 
    is_alert - boolean value, является последнее значение метрики аномальным или нет;
    df_today - DataFrame, содержащий информацию о значениях метрики в текущий день по пятнаднацатиминутным интервалам. 
    Помимо имевшихся изначально содержит столбцы: 
        - 'metric_values' - списки со значениями метрики, на основе которых рассчитываются интервалы;
        - 'median' - медианные значения метрики (в дальнейшем будут использоваться как ожидаемые или прогнозные значения);
        - 'q25' - значения 1 квартиля;
        - 'q75' - значения 3 квартиля;
        - 'iqr' - значения межквартильного размаха;
        - 'low' - нижние границы доверительных интервалов;
        - 'up' - верхние границы доверительных интервалов. 
    """
    df_today = df[df['date'] == df['date'].max()]
    
    metric_values = []
    for index in df_today.index:
        values = []
        for i in range(1, days + 1):
            for x in [-2, -1, 0, 1, 2]:
                values.append(df.iloc[index - i * 24 * 4 + x][metric])
        metric_values.append(values)
    df_today['metric_values'] = metric_values
    df_today = df_today.reset_index()
    
    df_today['median'] = df_today['metric_values'].apply(np.median)
    df_today['q25'] = df_today['metric_values'].apply(lambda x: np.quantile(x, 0.25))
    df_today['q75'] = df_today['metric_values'].apply(lambda x: np.quantile(x, 0.75))
    df_today['iqr'] = df_today['q75'] - df_today['q25']
    df_today['low'] = df_today['q25'] - a * df_today['iqr']
    df_today['up'] = df_today['q75'] + a * df_today['iqr']
    df_today['low'] = df_today['low'].rolling(5, center = True, min_periods = 1).mean()
    df_today['up'] = df_today['up'].rolling(5, center = True, min_periods = 1).mean()
   
    if df_today[metric].iloc[-1] < df_today['low'].iloc[-1] or df_today[metric].iloc[-1] > df_today['up'].iloc[-1]:
        is_alert = True
    else:
        is_alert = False

    return is_alert, df_today


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_nsmirnov_anomaly_alert():

    @task
    def run_alerts(chat=None):
        """
        Функция run_alerts достает из базы данных статитику по метрикам, проверяет метрики на аномальность с помощью функции check_anomaly, 
        а также при необходимости формирует и отправляет сообщение об аномалии и график в telegram.

        Параметры:
        chat - chat_id telegram для отправки сообщения
        """
        chat_id = chat or 926157354
        bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))

        read_connection = {'host': 'https://clickhouse.lab.karpov.courses',
                          'database':'simulator_20230720',
                          'user':'student', 
                          'password':os.environ.get("PASSWORD")
                         }
    
    # для удобства построения графиков в запрос можно добавить колонки date, hm
        query = ''' SELECT 
                        ts
                        ,feedtbl.date
                        ,feedtbl.hm
                        ,users_feed
                        ,likes
                        ,views
                        ,users_msg
                        ,messages
                    FROM
                        (SELECT
                            toStartOfFifteenMinutes(time) as ts
                            ,toDate(ts) as date
                            ,formatDateTime(ts, '%R') as hm
                            ,uniqExact(user_id) as users_feed
                            ,countIf(user_id, action = 'like') as likes
                            ,countIf(user_id, action = 'view') as views
                        FROM {db}.feed_actions
                        WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts) as feedtbl
                        FULL OUTER JOIN
                        (SELECT
                            toStartOfFifteenMinutes(time) as ts
                            ,toDate(ts) as date
                            ,formatDateTime(ts, '%R') as hm
                            ,uniqExact(user_id) as users_msg
                            ,count(user_id) as messages
                        FROM {db}.message_actions
                        WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts) as msgtbl
                        USING ts '''
        data = ph.read_clickhouse(query, connection = read_connection)

        metrics_names = {'users_feed':'👥Unique Feed Users👥',
                        'likes':'❤Number of Likes❤',
                        'views':'👀Number of Views👀',
                        'users_msg':'👥Unique Messenger Users👥',
                        'messages':'✉Number of Messages✉'}

        metrics_names_for_titles = {'users_feed':'Unique Feed Users',
                                    'likes':'Number of Likes',
                                    'views':'Number of Views',
                                    'users_msg':'Unique Messenger Users',
                                    'messages':'Number of Messages'}

        dash_links = {'users_feed':'http://superset.lab.karpov.courses/r/4306',
                    'likes':'http://superset.lab.karpov.courses/r/4308',
                    'views':'http://superset.lab.karpov.courses/r/4309',
                    'users_msg':'http://superset.lab.karpov.courses/r/4310',
                    'messages':'http://superset.lab.karpov.courses/r/4311'}
        
        # Формируем список метрик
        metrics = data.columns[3:]
        # Задаем параметры для расчета доверительных интервалов
        a = 1.5
        days = 7 # доверительный интервал на основе 4 дней (по умолчанию) высылал слишком много алертов по лайкам и просмотрам, попробуем поменять

        for metric in metrics:
            is_alert, df = check_anomaly(data, metric, a = a, days = days) # проверяем метрику на аномальность алгоритмом, описанным внутри функции check_anomaly()
            if is_alert:
            # Формируем сообщение
                msg = '''Проблема с метрикой {metric}:
    - Текущее значение = {current_value:,}
    - Отклонение от ожидаемого значения = {diff:.2%}
    - Отклонение от предыдущего значения = {diff_prev:.2%}
    - График в Superset: {link}
    - Оперативный дашборд: {dash}'''.format(metric = metrics_names[metric], 
                                        current_value = df[metric].iloc[-1], 
                                        diff = df[metric].iloc[-1] / df['median'].iloc[-1] - 1,
                                        diff_prev = df[metric].iloc[-1] / df[metric].iloc[-2] - 1,
                                        link = dash_links[metric],
                                        dash = 'http://superset.lab.karpov.courses/r/4312').replace(',', ' ')
            
                # Формируем график
                sns.set(style = 'whitegrid') # задаем стиль графика
                fig = plt.figure(figsize=(16, 10), dpi = 100) # задаем размер графика
                plt.tight_layout()

                ax = sns.lineplot(x = df["hm"], y = df[metric], label = metric, color = 'red') # Отображаем значения метрики в текущий день
                ax = sns.lineplot(x = df["hm"], y = df['median'], label = 'expected_value', color = 'blue', linestyle='dashed') # Отображаем ожидаемые значения метрики (медианные за предыдущие дни)
                ax = sns.lineplot(x = df["hm"], y = df['low'], color = 'tab:blue', alpha = 0.1) # Отображаем нижнюю границу доверительного интервала
                ax = sns.lineplot(x = df["hm"], y = df['up'], color = 'tab:blue', alpha = 0.1) # Отображаем верхнюю границу доверительного интервала
                ax.fill_between(df["hm"], df['low'], df['up'], alpha=0.2) # Закрашиваем область между границами доверительного интервала

                for ind, label in enumerate(ax.get_xticklabels()): # Этот цикл нужен чтобы разрядить подписи координат по оси Х
                    if ind % 8 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time') 
                ax.set(ylabel=None)

                ax.set_title('''Метрика {metric}\n
Параметры для интервалов: a = {a}, days = {days}'''.format(metric = metrics_names_for_titles[metric], a = a, days = days), fontweight='bold') # Задаем заголовок графика
                ax.set(ylim=(0, None)) # Задаем лимит для оси У

                # Формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metrics_names_for_titles[metric])
                plt.close()

                # Отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        
        
    run_alerts()
    

dag_nsmirnov_anomaly_alert = dag_nsmirnov_anomaly_alert()