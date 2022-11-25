import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221020'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-strievich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 10),
}

schedule_interval = '0 11 * * *' # каждый день в 11 утра

my_token = '5436697037:AAEQWgnOPkIYZj_nZdTn14-UbrvUIUWpdFA'
bot = telegram.Bot(token=my_token)

chat_id = -817148946

def the_last_day_metrics(df, chat_id=chat_id):
    """
    Функция формирует сообщение из датафрейма и
    отправляет его в телеграм
    """
    day = datetime.strftime(df.loc[:, 'day'][0], format='%d-%m-%Y') # parse date to string format
    dau = df.loc[:, 'DAU'][0]
    views = df.loc[:, 'views'][0]
    likes = df.loc[:, 'likes'][0]
    ctr = df.loc[:, 'CTR'][0]
    message = '\n'.join([f'Показатели за {day}',
                 f'DAU: {dau}',
                 f'Просмотры: {views}',
                 f'Лайки: {likes}',
                 f'CTR: {round(ctr, 3)}'
                ])
    bot.sendMessage(chat_id=chat_id, text=message)
    
def the_last_week_metrics(df, chat_id=chat_id):
    """
    Функция отрисовывает графики основных метрик за последнюю неделю
    и отправляет их в телеграм
    """
    sns.set(rc={'figure.figsize':(15, 10)})
    sns.set_context("notebook", rc={"lines.linewidth":2.0, 'lines.markersize': 7})
    fig, axes = plt.subplots(2, 2)
    fig.suptitle('Основные метрики за прошедшую неделю', fontsize='20')
    colors = sns.color_palette()
    sns.lineplot(data=df, ax=axes[0][0], color=colors[0], marker='o', x='day', y='DAU')
    sns.lineplot(data=df, ax=axes[0][1], color=colors[1], marker='o', x='day', y='views')
    sns.lineplot(data=df, ax=axes[1][0], color=colors[2], marker='o', x='day', y='likes')
    sns.lineplot(data=df, ax=axes[1][1], color=colors[3], marker='o', x='day', y='CTR')
    
    for ax in axes:
        for subax in ax:
            subax.tick_params('x', labelrotation=45)
            
    axes[0][0].set_title('DAU')
    axes[0][1].set_title('Views')
    axes[1][0].set_title('Likes')
    axes[1][1].set_title('CTR')
    fig.tight_layout() 

    plt_object = io.BytesIO()
    plt.savefig(plt_object)
    plt_object.seek(0)
    plt.object_name = 'metrics_yesterday.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plt_object)
        
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def super_dag():
    
    @task
    def extract_today_metrics():
        """
        Извлекает метрики за сегодня
        """
        q = """
        select
          toDate(time) as day,
          count(distinct user_id) as DAU,
          countIf(action = 'like') as likes,
          countIf(action = 'view') as views,
          likes / views as CTR
        from
          simulator_20221020.feed_actions
        where
          day = yesterday()
        group by
          day
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task 
    def extract_last_week_metrics():
        """
        Извлекает метрики за прошлую неделю 
        """
        q = """
        select
          toDate(time) as day,
          count(distinct user_id) as DAU,
          countIf(action = 'like') as likes,
          countIf(action = 'view') as views,
          likes / views as CTR
        from
          simulator_20221020.feed_actions
        where
          day between today() - 7
          and today() - 1
        group by
          day
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def report(df_1, df_2):
        """
        Таск отправляет сообщение и графики в телеграм
        """
        the_last_day_metrics(df_1)
        the_last_week_metrics(df_2) 
        
    today_df = extract_today_metrics()
    last_week_df = extract_last_week_metrics()
    report(today_df, last_week_df)
        
super_dag = super_dag()