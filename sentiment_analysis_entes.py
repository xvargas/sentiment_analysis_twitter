import re
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os.path

import jsonpickle as jsonpickle
import tweepy as tp
import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np

from datetime import datetime
from textblob import TextBlob
from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import twitter_credentials

"""
creado_por: Clelia Ximena Vargas Urrea
para: UNIR - Master en Big Data y Visual Analytics
fecha: septiembre 2019
trabajo: Implementación de software para la captura, análisis y visualización de la información obtenida de Twitter acerca del impacto ambiental
"""

class TwitterAuthenticator:

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterClient:
    def __init__(self, twitter_user=None, tweets_filename=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()

        self.twitter_api_client = API(self.auth, wait_on_rate_limit=True)

        self.listener = TwitterListener(tweets_filename)
        self.twitter_stream_client = Stream(self.auth, self.listener)

        self.twitter_user = twitter_user

    def set_twitter_user(self, user_name):
        self.twitter_user = user_name

    def save_user_timeline_tweets(self, user, user_timeline_file, user_timeline_list, user_timeline_filtered_file, user_timeline_filtered_list):
        with open(user_timeline_file, 'w') as file_tweet:
            for tweet in user_timeline_list:
                file_tweet.write(jsonpickle.encode(tweet._json, unpicklable=False) +
                                 '\n')

        with open(user_timeline_filtered_file, 'w') as file_tweet_filtered:
            for tweet in user_timeline_filtered_list:
                file_tweet_filtered.write(jsonpickle.encode(tweet._json, unpicklable=False) +
                                 '\n')

    def get_twitter_client_api(self):
        return self.twitter_api_client

    def get_twitter_stream_api(self):
        return self.twitter_stream_client

    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_api_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_user_timeline_hash_tags_tweets(self, search_tags):
        tweets = []

        for tweet in tp.Cursor(api.user_timeline, screen_name=self.twitter_user, tweet_mode='extended').items():
            if len(tweet._json['entities']['hashtags']) > 0:
                tweet_hashtags = [hashtag['text'] for hashtag in tweet._json['entities']['hashtags'] if
                                  hashtag['text'] in search_tags]
                if (len(tweet_hashtags) > 0):
                    tweets.append(tweet)

        return tweets

    def get_tweets_search(self, num_tweets, hash_tags_list=None):
        tweets = []
        for tweet in Cursor(self.twitter_api_client.search(hash_tags_list, count=num_tweets),
                            id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def define_hash_tags(self):
        hash_tag_list = ["amazonas", "biodiversidad", "eco", "ecofriendly", "ecologico",
                         "economiacircular", "energiasrenovables", "MedioAmbiente",
                         "naturaleza", "planeta", "plasticfree", "reciclaje", "renovables",
                         "reutiliza", "sostenible", "zerowaste", "cambioclimatico", "contaminacion",
                         "emergenciaclimatica", "plastico"]
        return hash_tag_list

    def get_tweets_search_more_pages(self, searchQuery, tweetsPerQry, language, until_date, sinceId, maxTweets, tweets_per_month):

        max_id = -1
        tweetCount = 0
        print("Descarga maxima de {0} tweets".format(maxTweets))

        other_tweets = []

        with open(tweets_per_month, 'w') as file_t:
            while tweetCount < maxTweets:
                try:
                    if (max_id <= 0):
                        if (not sinceId):
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date)
                        else:
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date,
                                                    since_id=sinceId)
                    else:
                        if (not sinceId):
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date,
                                                    max_id=str(max_id - 1))
                        else:
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date,
                                                    max_id=str(max_id - 1),
                                                    since_id=sinceId)
                    if not new_tweets:
                        print("No se encontraron mas tweets")
                        break
                    for tweet in new_tweets:
                        other_tweets.append(tweet)
                        file_t.write(jsonpickle.encode(tweet._json, unpicklable=False) +
                                '\n')
                    tweetCount += len(new_tweets)
                    print("{0} tweets descargados".format(tweetCount))
                    max_id = new_tweets[-1].id
                except tp.TweepError as e:
                    # Just exit if any error
                    print("Exception: " + str(e))
                    break

        print("Descargados {0} tweets, Almacenados en {1}".format(tweetCount, tweets_per_month))

        return other_tweets

    def get_tweets_user_hashtags_more_pages(self, searchQuery, tweetsPerQry, language, until_date, sinceId, maxTweets, tweets_per_user):

        max_id = -1
        tweetCount = 0
        print("Descarga maxima de {0} tweets".format(maxTweets))

        other_tweets = []

        with open(tweets_per_user, 'w') as file_t:
            while tweetCount < maxTweets:
                try:
                    if (max_id <= 0):
                        if (not sinceId):
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date)
                        else:
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date,
                                                    since_id=sinceId)
                    else:
                        if (not sinceId):
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date,
                                                    max_id=str(max_id - 1))
                        else:
                            new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language, until=until_date,
                                                    max_id=str(max_id - 1),
                                                    since_id=sinceId)
                    if not new_tweets:
                        print("No se encontraron mas tweets")
                        break
                    for tweet in new_tweets:
                        other_tweets.append(tweet)
                        file_t.write(jsonpickle.encode(tweet._json, unpicklable=False) +
                                '\n')
                    tweetCount += len(new_tweets)
                    print("{0} tweets descargados".format(tweetCount))
                    max_id = new_tweets[-1].id
                except tp.TweepError as e:
                    # Just exit if any error
                    print("Exception: " + str(e))
                    break

        print("Descargados {0} tweets, Almacenados en {1}".format(tweetCount, tweets_per_user))

        return other_tweets


class TwitterListener(StreamListener):

    def __init__(self, tweets_filename):
        self.tweets_filename = tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error: %s" % str(e))
        return True

    def on_error(self, status):
        if status == 420:
            return False
        print(status)


class Analyzer:

    def clean_tweet(self, tweet):
        tweet = re.sub(r'http?:.*$', '', tweet)
        tweet = re.sub(r'https?:.*$', '', tweet)

        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def convert_tweets_to_data_frame(self, tweets, user_flag=None):
        if isinstance(tweets[0],dict):
            df = pd.DataFrame(data=[tweet['text'] if user_flag is False else tweet['full_text'] for tweet in tweets], columns=['tweets'])
        else:
            df = pd.DataFrame(data=[tweet.text if user_flag is False else tweet.full_text for tweet in tweets], columns=['tweets'])
            converted_tweets = []
            for tweet in tweets:
                new_tweet = tweet.__dict__
                new_tweet['user'] = tweet.user if isinstance(tweet.user, dict) else tweet.user.__dict__
                converted_tweets.append(new_tweet)
            tweets = converted_tweets


        df['id'] = np.array([tweet['id'] for tweet in tweets])
        df['created_at'] = np.array([tweet['created_at'] for tweet in tweets])
        if user_flag is True:
            df['created_at_string'] = np.array([tweet['created_at'].strftime("%m/%Y")
                                                if isinstance(tweet['created_at'], datetime)
                                                else datetime.strptime(tweet['created_at'],"%a %b %d %H:%M:%S %z %Y").strftime("%m/%Y") for tweet in tweets])
        df['user_name'] = np.array([tweet['user']['screen_name'] for tweet in tweets])
        df['source'] = np.array([tweet['source'] for tweet in tweets])
        df['location'] = np.array([tweet['user']['location'] for tweet in tweets])
        df['likes'] = np.array([tweet['favorite_count'] for tweet in tweets])
        df['retweets'] = np.array([tweet['retweet_count'] for tweet in tweets])

        return df

    def analyze_sentiment(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))

        if analysis.sentiment.polarity > 0:
            return 1
        elif analysis.sentiment.polarity == 0:
            return 0
        else:
            return -1


if __name__ == '__main__':
    # Authenticate using config.py and connect to Twitter Streaming API.
    tweets_filename = "initial_tweets_file_sep_18_t_rt.json"

    # 1. crear cliente de twitter
    twitter_client = TwitterClient(tweets_filename=tweets_filename)

    # 2. se realiza consulta de tweets entre fechas con el listado de palabras definidas
    hash_tags_list = twitter_client.define_hash_tags()

    api = twitter_client.get_twitter_client_api()

    tweet_analyzer = Analyzer()

    hash_tag_list = ['amazonas','biodiversidad','eco','ecofriendly',
                     'ecologico','economiacircular','energiasrenovables',
                     'MedioAmbiente','plasticfree','reciclaje','renovables',
                     'sostenible','cambioclimatico','contaminacion','emergenciaclimatica','plastico']
    searchQuery = 'amazonas OR biodiversidad OR eco OR ecofriendly OR ecologico OR OR economiacircular OR energiasrenovables OR MedioAmbiente OR plasticfree OR reciclaje OR renovables OR sostenible OR cambioclimatico OR contaminacion OR emergenciaclimatica OR plastico"'

    # se convierte a DataFrame
    user_list = ['ONUMedioAmb','CMNUCC']

    # ############################################################# timeline
    # segunda parte: por cada usuario buscar la linea de tiempo

    user_timeline_filename = 'user_timeline_filename_'
    user_timeline_filtered_filename = 'user_timeline_filtered_filename_'

    dictionary_user_tweets = {}

    for user in user_list:
        # inicializar listas
        tweets_per_user = []
        tweets_per_user_filtered = []

        if os.path.exists(user_timeline_filename+str(user)+'.json') is True:
            with open(user_timeline_filename+str(user)+'.json', 'r') as f:
                for line in f:
                    tweets_per_user.append(json.loads(line))
            with open(user_timeline_filtered_filename+str(user)+'.json', 'r') as f:
                for line in f:
                    tweets_per_user_filtered.append(json.loads(line))
        else:

            for page in tp.Cursor(api.user_timeline, screen_name=user, include_rts=False,
                                  tweet_mode='extended').pages():
                for tweet in page:
                    tweets_per_user.append(tweet)
                    word_flag = False
                    for word in hash_tag_list:
                        if word in tweet.full_text:
                            word_flag = True
                            break
                    if word_flag:
                        tweets_per_user_filtered.append(tweet)

            twitter_client.save_user_timeline_tweets(user, user_timeline_filename+str(user)+'.json',
                                                     tweets_per_user,
                                                     user_timeline_filtered_filename+str(user)+'.json',
                                                     tweets_per_user_filtered)

        print('User: '+user+' Tweets: '+str(len(tweets_per_user))+' Filtered tweets: '+str(len(tweets_per_user_filtered)))

        # graficar
        if len(tweets_per_user) > 0:
            tweet_list = tweet_analyzer.convert_tweets_to_data_frame(tweets_per_user, True)
            tweet_text_df = pd.DataFrame(data=tweet_list, columns=['created_at_string', 'created_at', 'id'])
            unsorted_dict = tweet_text_df['created_at_string'].value_counts().to_dict()
            original_dict = {}

            for item in unsorted_dict:
                original_dict[datetime.strptime(item, "%m/%Y")] = unsorted_dict[item]

            original_dict = sorted(original_dict)

            x_values = []
            y_values = []
            for item in original_dict:
                x_values.append(item.strftime('%m/%Y'))

            y_values = [unsorted_dict[item] for item in x_values]

            x = np.array(x_values)
            y = np.array(y_values)

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=x, y=y, name="Tweets sin filtrar",
                                     line_color='deepskyblue'))

            # Tweets filtrados
            tweet_list_filtered = tweet_analyzer.convert_tweets_to_data_frame(tweets_per_user_filtered, True)
            tweet_text_df = pd.DataFrame(data=tweet_list_filtered, columns=['created_at_string', 'created_at', 'id'])
            unsorted_dict = tweet_text_df['created_at_string'].value_counts().to_dict()
            original_dict = {}

            for item in unsorted_dict:
                original_dict[datetime.strptime(item, "%m/%Y")] = unsorted_dict[item]

            original_dict = sorted(original_dict)

            x_values = []
            y_values = []
            for item in original_dict:
                x_values.append(item.strftime('%m/%Y'))

            y_values = [unsorted_dict[item] for item in x_values]

            x = np.array(x_values)
            y = np.array(y_values)

            fig.add_trace(go.Scatter(x=x, y=y, name="Tweets Medio Ambiente",
                                     line_color='green'))

            fig.update_layout(title_text='Tweets de '+user,
                              xaxis_rangeslider_visible=True)

            fig.show()

            dictionary_user_tweets[user] = [tweet_list, tweet_list_filtered]

    print(len(dictionary_user_tweets))

    sentimentlist = []
    i = 0
    for user_d in dictionary_user_tweets:
        if len(dictionary_user_tweets[user_d][1]['created_at_string'].value_counts()) > 1:

            dataframe_analysis = dictionary_user_tweets[user_d][1]
            dataframe_analysis['cleaned_text'] = np.array([tweet_analyzer.clean_tweet(tweet) for tweet in dataframe_analysis['tweets']])

            dataframe_analysis['polarity'] = np.array(
                [TextBlob(tweet).sentiment.polarity for tweet in dataframe_analysis['cleaned_text']])

            dataframe_analysis['subjectivity'] = np.array(
                [TextBlob(tweet).sentiment.subjectivity for tweet in dataframe_analysis['cleaned_text']])

            i = i + 1

    fig_general = go.Figure()

    df_general = pd.DataFrame({'created_at_string': [], 'created_at': [], 'polarity': [], 'user': []})

    for user_d in dictionary_user_tweets:
        if len(dictionary_user_tweets[user_d][1]['created_at_string'].value_counts()) > 1:

            tweet_text_df = pd.DataFrame(data=dictionary_user_tweets[user_d][1].reindex(index=dictionary_user_tweets[user_d][1].index[::-1]), columns=['created_at_string', 'created_at', 'polarity'])
            fig = px.scatter(tweet_text_df, x="created_at_string", y="polarity", title='Polaridad '+user_d)
            fig.update_xaxes(title_text='Fecha creación', showline=True, linewidth=2, linecolor='black');
            fig.update_yaxes(title_text='Polaridad', showline=True, linewidth=2, linecolor='black');
            fig.show()

            total = 0;
            positivos = 0;
            negativos = 0;
            neutros = 0;
            for item in tweet_text_df['polarity']:
                if item < 0:
                    negativos = negativos + 1;
                elif item > 0:
                    positivos = positivos + 1;
                else:
                    neutros = neutros + 1;

            total = positivos + negativos + neutros
            print('User: '+user_d+' Total Tweets: '+str(total)+' Positivos: '+str(positivos)+' Negativos: '+str(negativos)+' Neutros: '+str(neutros))

            tweet_text_df['user'] = user_d
            df_general = df_general.append(tweet_text_df, ignore_index=True)

    print('Fin de aplicación')


