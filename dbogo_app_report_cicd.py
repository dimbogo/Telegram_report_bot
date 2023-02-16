import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

my_token = '' # here you need to replace it with your bot's token
bot = telegram.Bot(token=my_token) # getting access


# chat-id can be obtained by sending a message to the bot using the link https://api.telegram.org/bot<your_bot_token>/getUpdates or using the bot.getUpdates() method
chat_id = ''

# connection to Clickhouse database
connection = {'host': '',
                              'database':'',
                              'user':'',
                              'password':''
                             }

def dbogoslovtsev_app_report_cicd():
    
    def extract():
        # our query
        q = """SELECT * EXCEPT (retained_users),
                    retained_users / (DAU_ads + DAU_organic) AS daily_retention_rate
                FROM
                (SELECT *
                FROM (SELECT toStartOfDay(toDateTime(time)) AS Date,
                                       countIf(DISTINCT user_id, source = 'ads') AS DAU_ads,
                                       countIf(DISTINCT user_id, source = 'organic') AS DAU_organic,
                                       countIf(user_id, action = 'view' AND os = 'Android') AS Views_android,
                                       countIf(user_id, action = 'view' AND os = 'iOS') AS Views_ios,
                                       countIf(user_id, action = 'like' AND os = 'Android') AS Likes_android,
                                       countIf(user_id, action = 'like' AND os = 'iOS') AS Likes_ios,
                                       countIf(user_id, action = 'like') / countIf(user_id, action = 'view') AS CTR,
                                       count(DISTINCT post_id) AS posts_interacted
                FROM simulator_20221220.feed_actions
                GROUP BY Date
                HAVING Date >= today() - 7
                ) as t1 JOIN 
                (SELECT toStartOfDay(toDateTime(time)) AS Date,
                        countIf(user_id, os = 'Android') AS messages_sent_android,
                        countIf(user_id, os = 'iOS') AS messages_sent_ios
                FROM simulator_20221220.message_actions
                GROUP BY Date
                HAVING Date >= today() - 7
                ) as t2 USING(Date)) as temp1 JOIN
                (SELECT toStartOfDay(toDateTime(this_day)) AS Date,
                       sum(num_users) AS retained_users
                FROM
                  (SELECT this_day,
                          previous_day, -uniq(user_id) as num_users,
                                          status
                   FROM
                     (SELECT user_id,
                             groupUniqArray(toStartOfDay(toDate(time))) as days_visited,
                             addDays(arrayJoin(days_visited), +1) this_day,
                             if(has(days_visited, this_day) = 1, 'retained', 'gone') as status,
                             addDays(this_day, -1) as previous_day
                      FROM simulator_20221220.feed_actions
                      WHERE toDate(time) >= today() - 8
                      group by user_id
                      )
                   where status = 'gone'
                   group by this_day,
                            previous_day,
                            status
                   HAVING this_day != addDays(toStartOfDay(today()), +1)
                   union all SELECT this_day,
                                    previous_day,
                                    toInt64(uniq(user_id)) as num_users,
                                    status
                   FROM
                     (SELECT user_id,
                             groupUniqArray(toStartOfDay(toDate(time))) as days_visited,
                             arrayJoin(days_visited) this_day,
                             if(has(days_visited, addDays(this_day, -1)) = 1, 'retained', 'new') as status,
                             addDays(this_day, -1) as previous_day
                      FROM simulator_20221220.feed_actions
                      WHERE toDate(time) >= today() - 8
                      group by user_id
                      )
                   group by this_day,
                            previous_day,
                            status) AS virtual_table
                WHERE status = 'retained'
                GROUP BY toStartOfDay(toDateTime(this_day))
                ) as temp2 USING(Date)
                ORDER BY Date ASC"""
        
        #convert query to pandas dataframe
        df = ph.read_clickhouse(q, connection=connection)        
        return df
    
    def transform_msg(df):
        # yesterday's key metrics
        msg = (f'Application performance report for {df.iloc[-2].Date:%d/%m/%Y}\n'
               f'DAU - {int(df.iloc[-2].DAU_ads + df.iloc[-2].DAU_organic):,}\n'
              f'Views - {int(df.iloc[-2].Views_android + df.iloc[-2].Views_ios):,}\n'
              f'Likes - {int(df.iloc[-2].Likes_android + df.iloc[-2].Likes_ios):,}\n'
              f'CTR - {round(df.iloc[-2].CTR, 3)}'
              f'Posts - {df.iloc[-2].posts_interacted}'
              f'Messages - {int(df.iloc[-2].messages_sent_android + df.iloc[-2].messages_sent_ios)}'
              f'Retention rate - {round(df.iloc[-2].daily_retention_rate, 3)}')        
        return msg

    def transform_plot_dau(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("Unique users", size=16, labelpad = 10)
        plt.title("DAU by sources of traffic", size=24)
        plt.stackplot(df.Date, df.DAU_ads, df.DAU_organic, labels = ['ads', 'organic'],  alpha=0.8)
        plt.legend()
        DAU_io = io.BytesIO()#creating an empty clipboard file
        plt.savefig(DAU_io)#saving chart to clipboard
        DAU_io.seek(0)#moving cursor to the beginning
        DAU_io.name = 'DAU.png'
        plt.close()
        return DAU_io
    
    def transform_plot_views(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("Views", size=16, labelpad = 10)
        plt.title("Daily views by OS", size=24)
        color_map = ['#007aff', '#32DE84']
        plt.stackplot(df.Date, df.Views_ios, df.Views_android, labels = ['iOS', 'Android'],  alpha=0.8, colors = color_map)
        plt.legend()
        views_io = io.BytesIO()
        plt.savefig(views_io)
        views_io.seek(0)
        views_io.name = 'Views.png'
        plt.close()
        return views_io

    def transform_plot_likes(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("Likes", size=16, labelpad = 10)
        plt.title("Daily likes by OS", size=24)
        color_map = ['#007aff', '#32DE84']
        plt.stackplot(df.Date, df.Likes_ios, df.Likes_android, labels = ['iOS', 'Android'],  alpha=0.8, colors = color_map)
        plt.legend()
        likes_io = io.BytesIO()
        plt.savefig(likes_io)
        likes_io.seek(0)
        likes_io.name = 'Likes.png'
        plt.close()
        return likes_io
    
    def transform_plot_ctr(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("CTR", size=16, labelpad = 10)
        plt.title("CTR", size=24)
        sns.lineplot(x='Date', y='CTR', data = df, color = 'y')
        CTR_io = io.BytesIO()
        plt.savefig(CTR_io)
        CTR_io.seek(0)
        CTR_io.name = 'CTR.png'
        plt.close()
        return CTR_io
    
    def transform_plot_posts(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("Posts", size=16, labelpad = 10)
        plt.title("Daily viewed posts", size=24)
        sns.lineplot(x='Date', y='posts_interacted', data = df, color = 'purple')
        posts_io = io.BytesIO()
        plt.savefig(posts_io)
        posts_io.seek(0)
        posts_io.name = 'posts.png'
        plt.close()
        return posts_io
    
    def transform_plot_messages(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("Messages", size=16, labelpad = 10)
        plt.title("Daily sent messages by OS", size=24)
        color_map = ['#007aff', '#32DE84']
        plt.stackplot(df.Date, df.messages_sent_ios, df.messages_sent_android, labels = ['iOS', 'Android'],  alpha=0.8, colors = color_map)
        plt.legend()
        messages_io = io.BytesIO()
        plt.savefig(messages_io)
        messages_io.seek(0)
        messages_io.name = 'Messages.png'
        plt.close()
        return messages_io
    
    def transform_plot_retention_rate(df):
        sns.set(rc={'figure.figsize':(12,8)})
        sns.set_style("whitegrid")
        plt.xlabel("Date", size=16, labelpad = 10)
        plt.ylabel("Retention rate", size=16, labelpad = 10)
        plt.title("Daily Retention rate", size=24)
        sns.lineplot(x='Date', y='daily_retention_rate', data = df, color = 'orange')
        retention_io = io.BytesIO()
        plt.savefig(retention_io)
        retention_io.seek(0)
        retention_io.name = 'retention.png'
        plt.close()
        return retention_io

    def transform_io_array(dau, views, likes, ctr, posts, messages, retention_rate, msg):
        io_array = [dau, views, likes, ctr, posts, messages, retention_rate]
        # Converting each photo to InputMedia format and forming an array
        array = []
        for num, photo in enumerate(io_array):
            array.append(telegram.InputMediaPhoto(media = photo, caption = msg if num == 0 else ''))# Only for the first photo make a description so that it is displayed in the post under the photos
        return array

    def load_to_telegram(array):
        bot.sendMediaGroup(chat_id=chat_id, media = array)
        
    df = extract()
    msg = transform_msg(df)
    dau = transform_plot_dau(df)
    views = transform_plot_views(df)
    likes = transform_plot_likes(df)
    ctr = transform_plot_ctr(df)
    posts = transform_plot_posts(df)
    messages = transform_plot_messages(df)
    retention_rate = transform_plot_retention_rate(df)
    array = transform_io_array(dau, views, likes, ctr, msg)
    load_to_telegram(array)
    
dbogoslovtsev_app_report_cicd()