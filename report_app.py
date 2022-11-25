import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, date

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

default_args = {
    'owner': 'e-strievich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 13),
}

schedule_interval = '0 11 * * *' # каждый день в 11 утра

my_token = '5436697037:AAEQWgnOPkIYZj_nZdTn14-UbrvUIUWpdFA'
bot = telegram.Bot(token=my_token)

chat_id = -817148946

def read_ch(q):
    """
    Функция для чтения запросов из CH
    """
    df = ph.read_clickhouse(q, connection=connection)
    return df

def final_message(dau_message,
                 new_users_message,
                 sum_activity_message,
                 chat_id=chat_id
                 ):
    """ 
    Формирует сообщения по метрикам и отправляет
    итоговое сообщение в телеграм
    """
    today = date.today().strftime(format='%d-%m-%Y')
    title = f'ОТЧЕТ ПО ПРИЛОЖЕНИЮ ЗА {today}'
    message = '\n'.join([title, '', '', dau_message, '', '', new_users_message, '', '', sum_activity_message])
    
    bot.sendMessage(chat_id=chat_id, text=message)
    
def plot_metrics(df_feed_avg, df_mes_avg, df_both_dau,
                 df_during_day_feed, df_during_day_mes,
                 df_ctr, df_retention):
    """
    Функция отрисовывает основные метрики и отправляет в телеграм
    """
    yesterday = (date.today() - timedelta(1)).strftime(format='%d-%m-%Y')
    # устанаваливаем параметры графиков
    sns.set(rc={'figure.figsize':(20, 20)})
    sns.set_context("notebook", font_scale=1.5, \
                        rc={"lines.linewidth":2.0, 'lines.markersize': 7})
    # отрисовываем основные метрики
    fig, axes = plt.subplots(2, 2)
    
    
    sns.lineplot(data=df_feed_avg, x='day', y='avg_activity', marker='o',\
                 color='#DC3535', label='Лента новостей', ax=axes[0][0])
    sns.lineplot(data=df_mes_avg, x='day', y='avg_activity', marker='o',\
                 color='#0D4C92', label='Мессенджер', ax=axes[0][0])
    axes[0][0].set_title('Среднее число действий на пользователя за последнюю неделю', fontsize='20')
    axes[0][0].set_ylabel('Количество действий', fontsize='20')
    axes[0][0].set_xlabel('День', fontsize='20')
    
    sns.lineplot(data=df_both_dau, x='day', y='both_dau', marker='o',\
                 color='#0E5E6F',
                 ax=axes[0][1])
    axes[0][1].set_title('DAU обоих сервисов за последнюю неделю', fontsize='20')
    axes[0][1].set_ylabel('Число пользователей', fontsize='20')
    axes[0][1].set_xlabel('День', fontsize='20')
    
    sns.lineplot(data=df_during_day_feed, x='hour', y='activity', marker='o',\
                 color='#E14D2A', ax=axes[1][0], label='Лента новостей')
    sns.lineplot(data=df_during_day_mes, x='hour', y='activity', marker='o',\
                 color='#001253', ax=axes[1][0], label='Мессенджер')
    axes[1][0].set_title(f'Суммарная активность в течение дня за {yesterday}', fontsize='20')
    axes[1][0].set_ylabel('Число пользователей', fontsize='20')
    axes[1][0].set_xlabel('Час', fontsize='20')
    axes[1][0].set_xticks(range(0, 24, 2))
    
    sns.lineplot(data=df_ctr, x='day', y='CTR', marker='o', hue='week', palette='viridis', ax=axes[1][1])
    axes[1][1].set_title('CTR за последние 14 и 7 дней', fontsize='20')
    axes[1][1].set_ylabel('CTR', fontsize='20')
    axes[1][1].set_xlabel('День', fontsize='20')
    axes[1][1].lines[0].set_linestyle('--')
    
    for ax in axes:
        for subax in ax:
            subax.tick_params('x', labelrotation=45)
            
    fig.tight_layout()
    
    plt_object = io.BytesIO()
    plt.savefig(plt_object)
    plt_object.seek(0)
    plt.object_name = 'report_app.png'
    bot.sendPhoto(chat_id=chat_id, photo=plt_object)
    
    # retention
    sns.set(rc={'figure.figsize':(14, 7)})
    fig, ax = plt.subplots(1, 1)
    sns.lineplot(data=df_retention, x='day', y='retention', marker='o', color = "#432C7A", ax=ax)
    ax.set_title('Retention пользователей по обоим сервисам, зарегистрировашихся неделю назад', fontsize='17')
    ax.set_ylabel('Число пользователей', fontsize='17')
    ax.set_xlabel('День', fontsize='17')
    fig.tight_layout()
    
    plt_object = io.BytesIO()
    plt.savefig(plt_object)
    plt_object.seek(0)
    plt.object_name = 'retention.png'
    bot.sendPhoto(chat_id=chat_id, photo=plt_object)
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_dag():
    
    @task
    def extract_dau_feed():
        """
        Извлекает DAU юзеров новостной ленты
        """
        q_dau_feed = """
        select 
            toDate(time) as day,
            count(distinct user_id) as DAU
        from simulator_20221020.feed_actions
        group by toDate(time)
        having day = today() or day = today() - 1 or day = today() - 7
        """
   
        df_dau_feed = read_ch(q_dau_feed)

        return df_dau_feed
    
    @task
    def extract_dau_mes():
        """
        Извлекает DAU юзеров мессенджера
        """
        q_dau_mes = """
        select 
            toDate(time) as day,
            count(distinct user_id) as DAU
        from simulator_20221020.message_actions
        group by toDate(time)
        having day = today() or day = today() - 1 or day = today() - 7
        """
        
        df_dau_mes = read_ch(q_dau_mes)
        
        return df_dau_mes
    
    @task
    def extract_dau_both():
        """
        Извлекает DAU юзеров, которые пользуются и тем, и другим
        """
        q_dau_both = """
        select
            count(distinct user_id) as DAU
            from 
        (select user_id, toDate(time) as day from simulator_20221020.feed_actions) t1
        join 
        (select user_id from simulator_20221020.message_actions) t2
        on t1.user_id = t2.user_id
        group by day
        having day = today() or day = today() - 1 or day = today() - 7
        """
        
        df_dau_both = read_ch(q_dau_both)
        
        return df_dau_both
    
    @task
    def extract_new_users_feed():
        """
        Извлекает новых юзеров за вчерашний день и неделю назад в новостной ленте
        """
        q_new_feed = """
        select
            start_day,
            count(distinct user_id) as new_users
        from
        (
          select
            user_id,
            min(toDate(time)) as start_day
          from
            simulator_20221020.feed_actions
          group by
            user_id
          having
            start_day = today() - 1 or start_day = today() - 7
        )
        group by start_day
        order by start_day
        """
        
        df_new_feed = read_ch(q_new_feed)

        return df_new_feed
        
    @task
    def extract_new_users_mes():
        """
        Извлекает новых юзеров за вчерашний день и неделю назад в мессенджере
        """
        q_new_mes = """
        select
            start_day,
            count(distinct user_id) as new_users
        from
        (
          select
            user_id,
            min(toDate(time)) as start_day 
          from
            simulator_20221020.message_actions
          group by
            user_id
          having
            start_day = today() - 1 or start_day = today() - 7
        )
        group by start_day
        order by start_day
        """
        
        df_new_mes = read_ch(q_new_mes)

        return df_new_mes
        
    @task   
    def extract_sum_activity():
        """
        Функция извлекает суммарную активность по 
        обоим сервисам
        """
        q_activity = """
        select
            day,
            sum(activity) as sum_activity
        from 

        (select 
            count(user_id) as activity,
            toDate(time) as day
        from simulator_20221020.feed_actions
        where toDate(time) = today() - 1 or toDate(time) = today() - 7
        group by day
        
        union all

        select
            count(user_id) as activity,
            toDate(time) as day
        from simulator_20221020.message_actions
        where toDate(time) = today() - 1 or toDate(time) = today() - 7
        group by day
        )
        group by day
        order by day
        """
        
        df_activity = read_ch(q_activity)

        return df_activity
    
    @task
    def extract_avg_activity_feed():
        """
        Функция извлекает данные о средней активности
        пользователей в ленте новостей и мессенджере
        за прошлую неделю
        """
        q_feed_avg = """
        select 
            toDate(time) as day,
            count(user_id) / count(distinct user_id) as avg_activity
            from simulator_20221020.feed_actions
        where day between today() - 7 and today() - 1
        group by day
        """
        
        df_feed_avg = read_ch(q_feed_avg)

        return df_feed_avg
    
    @task
    def extract_avg_activity_mes():
        """
        Функция извлекает данные о средней активности
        пользователей в мессенджере за прошлую неделю
        """
        q_mes_avg = """
        select 
            toDate(time) as day,
            count(user_id) / count(distinct user_id) as avg_activity
            from simulator_20221020.message_actions
         where day between today() - 7 and today() - 1  
        group by day
        """
        
        df_mes_avg = read_ch(q_mes_avg)

        return df_mes_avg 
    
    @task
    def extract_both_dau():
        """
        Функия извлекает DAU по обоим сервисвам за прошлую неделю
        """
        q_both_dau = """
        select
            sum(dau) as both_dau,
            day
          from
            (
              select
                count(distinct user_id) as dau,
                toDate(time) as day
              from
                simulator_20221020.feed_actions
              where
                day >= today() - 7
                and day <= today() - 1
              group by
                day
              union all
              select
                count(distinct user_id) as dau,
                toDate(time) as day
              from
                simulator_20221020.message_actions
              where
                day >= today() - 7
                and day <= today() - 1
              group by
                day
            )
          group by day
        """
        
        df_both_dau = read_ch(q_both_dau)

        return df_both_dau
    
    @task
    def extract_act_during_day_feed():
        """ 
        Функция извлекает активность в течение дня
        по новостной ленте за прошедший день
        """
        q_during_day_feed = """
        select 
            toStartOfHour(time) as hour,
            count(user_id) as activity
        from simulator_20221020.feed_actions
        where toDate(time) = today() - 1
        group by hour, toDate(time)
        order by hour 
        """

        df_during_day_feed = read_ch(q_during_day_feed)
        df_during_day_feed['hour'] = df_during_day_feed['hour'].dt.strftime('%d-%m %H:00')
        
        return df_during_day_feed
    
    @task
    def extract_act_during_day_mes():
        """ 
        Функция извлекает активность в течение дня
        по мессенджеру за прошедший день
        """
        q_during_day_mes = """
        select 
            toStartOfHour(time) as hour,
            count(user_id) as activity
        from simulator_20221020.message_actions
        where toDate(time) = today() - 1
        group by hour, toDate(time)
        order by hour 
        """

        df_during_day_mes = read_ch(q_during_day_mes)
        df_during_day_mes['hour'] = df_during_day_mes['hour'].dt.strftime('%d-%m %H:00')
        
        return df_during_day_mes
    
    @task
    def extract_ctr():
        q_ctr = """
        select
          toDate(time) as day,
          count(distinct user_id) as DAU,
          countIf(action = 'like') as likes,
          countIf(action = 'view') as views,
          likes / views as CTR,
          if(day between today() - 7 and today() - 1, 'last_7days', 'last_14days') as week
        from
          simulator_20221020.feed_actions
        where
          (day between today() - 7
          and today() - 1) or
          (day between today() - 14 and today() - 8)
        group by
          day
        order by day
        """
        
        df_ctr = read_ch(q_ctr)
        df_ctr['day'] = df_ctr.apply(lambda x: x['day'] + timedelta(7) if x['week'] == 'last_14days' else x['day'], axis=1)
        
        return df_ctr
    
    @task
    def extract_retention():
        """
        Функция извлекает retention
        пользователей, зарегистрировавшихся неделю назад
        """
        q_retention_both = """
        with start_users_both_feed as (
        select distinct user_id from (
        select 
            user_id,
            min(toDate(time)) as start_day
            from simulator_20221020.feed_actions
            group by user_id
            having start_day = today() - 7
            )
        ),
        start_users_both_mes as (
        select distinct user_id from (
            select 
            user_id,
            min(toDate(time)) as start_day
            from simulator_20221020.message_actions
            group by user_id
            having start_day = today() - 7 
            )
        ),

        retention_feed as (
        select 
            toDate(time) as day,
            count(distinct user_id) as retention
        from simulator_20221020.feed_actions
            where user_id in start_users_both_feed
            group by day
        ),
        retention_mes as (
        select
            toDate(time) as day,
            count(distinct user_id) as retention
            from simulator_20221020.message_actions
            where user_id in start_users_both_mes
            group by day
        )
        select 
            day,
            sum(retention) as retention
        from (
        select * from retention_feed
        union all
        select * from retention_mes)
        group by day
        """
        
        df_retention = read_ch(q_retention_both)
        
        return df_retention
        
    @task
    def dau(df_feed, df_mes, df_both):
        """
        Рассчитывает DAU по каждому сервису за прошлый день и прошлую неделю
        и возвращает сообщение
        """
        day = datetime.strftime(df_feed['day'][0], format='%d-%m-%Y')
        last_week_dau_feed = df_feed['DAU'][0].astype('int64')
        yest_day_feed = df_feed['DAU'][1].astype('int64')
        currect_dau_feed = df_feed['DAU'][2].astype('int64')

        last_week_dau_mes = df_mes['DAU'][0].astype('int64')
        yest_day_mes = df_mes['DAU'][1].astype('int64')
        currect_dau_mes = df_mes['DAU'][2].astype('int64')

        last_week_dau_both = df_both['DAU'][0].astype('int64')
        yest_day_both = df_both['DAU'][1].astype('int64')
        currect_dau_both = df_both['DAU'][2].astype('int64')

        message_feed = '\n'.join([f'DAU ПО ЛЕНТЕ НОВОСТЕЙ',
                                  '',
                                  f'DAU на текущий момент: {currect_dau_feed}',
                                  f'Относительное изменение DAU за вчерашний день по сравнению с прошлой неделей: ' 
                                  f'{round((yest_day_feed - last_week_dau_feed) / last_week_dau_feed * 100, 2)}%'
                                 ])
        message_mes = '\n'.join([f'DAU ПО СЕРВИСУ ОТПРАВКИ СООБЩЕНИЙ',
                                 '',
                                 f'DAU на текущий момент: {currect_dau_mes}',
                                 f'Относительное изменение DAU за вчерашний день по сравнению с прошлой неделей: '
                                 f'{round((yest_day_mes - last_week_dau_mes) / last_week_dau_mes * 100, 2)}%'
                                ])
        message_both = '\n'.join([f'DAU ПОЛЬЗОВАТЕЛЕЙ, ИСПОЛЬЗОВАВШИХ ОБА СЕРВИСА',
                                  '',
                                  f'DAU на текущий момент: {currect_dau_both}',
                                  f'Относительное изменение DAU за вчерашний день по сравнению с прошлой неделей: '
                                  f'{round((yest_day_both - last_week_dau_both) / last_week_dau_both * 100, 2)}%'
                                 ])
        message = '\n'.join([message_feed, '', '', message_mes, '', '', message_both])
        
        return message
         
    @task   
    def new_users(df_new_feed, df_new_mes):
        """
        Рассчитывает количество новых юзеров по каждому сервису за прошлый день и прошлую неделю
        и возвращает сообщение
        """
        new_users_last_week_feed = df_new_feed['new_users'][0].astype('int64')
        new_users_yest_feed = df_new_feed['new_users'][1].astype('int64')

        new_users_last_week_mes = df_new_mes['new_users'][0].astype('int64')
        new_users_yest_mes = df_new_mes['new_users'][1].astype('int64')

        message_feed = '\n'.join([f'КОЛИЧЕСТВО НОВЫХ ЮЗЕРОВ В НОВОСТНОЙ ЛЕНТЕ',
                        '',
                        f'Количество новых юзеров  за вчерашний день: {new_users_yest_feed}',
                        f'Относительное изменение новых юзеров по сравнению с прошлой неделей: '
                        f'{round((new_users_yest_feed - new_users_last_week_feed) / new_users_last_week_feed * 100, 2)}%']
                                 )
        message_mes = '\n'.join([f'КОЛИЧЕСТВО НОВЫХ ЮЗЕРОВ В МЕССЕНДЖЕРЕ',
                        '',
                        f'Количество новых юзеров  за вчерашний день: {new_users_yest_mes}',
                        f'Относительное изменение новых юзеров по сравнению с прошлой неделей: '
                        f'{round((new_users_yest_mes - new_users_last_week_mes) / new_users_last_week_mes * 100, 2)}%']
                               )
        message = '\n'.join([message_feed, '', '', message_mes])
        
        return message
    
    @task
    def sum_activity(df_activity):
        """
        Рассчитывает суммарную активность по каждому сервису за прошлый день и прошлую неделю
        и возвращает сообщение
        """
        sum_activity_week_ago = df_activity['sum_activity'][0].astype('int64')
        sum_activity_yest = df_activity['sum_activity'][1].astype('int64')

        message = '\n'.join([f'СУММАРНАЯ АКТИВНОСТЬ ПОЛЬЗОВАТЕЛЕЙ В ОБОИХ СЕРВИСАХ',
                             '',
                             f'Суммарная активность за вчерашний день: {sum_activity_yest}',
                             f'Относительное изменение суммарной активности по сравнению с прошлой неделей: '
                             f'{round((sum_activity_yest - sum_activity_week_ago) / sum_activity_week_ago * 100, 2)}%']
                           )

        return message
    
    @task
    def make_report(dau_message,
                    new_users_message,
                    sum_activity_message,
                    df_feed_avg, df_mes_avg, df_both_dau,
                    df_during_day_feed, df_during_day_mes,
                    df_ctr, df_retention
                    ):
        """
        Таск отправляет сообщение и графики в телеграм
        """
        final_message(dau_message,
                 new_users_message,
                 sum_activity_message,
                 chat_id=chat_id)
        plot_metrics(df_feed_avg, df_mes_avg, df_both_dau,
                 df_during_day_feed, df_during_day_mes,
                 df_ctr, df_retention)
          
    df_feed = extract_dau_feed()
    df_mes = extract_dau_mes()
    df_both = extract_dau_both()
    df_new_feed = extract_new_users_feed()
    df_new_mes = extract_new_users_mes()
    df_activity = extract_sum_activity()
    df_feed_avg = extract_avg_activity_feed()
    df_mes_avg = extract_avg_activity_mes()
    df_both_dau = extract_both_dau()
    df_during_day_feed = extract_act_during_day_feed()
    df_during_day_mes = extract_act_during_day_mes()
    df_ctr = extract_ctr()
    df_retention = extract_retention()
    dau_message = dau(df_feed, df_mes, df_both)
    new_users_message = new_users(df_new_feed, df_new_mes)
    sum_activity_message = sum_activity(df_activity)
    make_report(dau_message, new_users_message, sum_activity_message,
                df_feed_avg, df_mes_avg, df_both_dau, df_during_day_feed,
                df_during_day_mes, df_ctr, df_retention)

report_dag = report_dag()   