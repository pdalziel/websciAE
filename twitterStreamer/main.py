import logging
from configparser import ConfigParser

import tweepy
from pymongo import MongoClient
from tweepy import Stream

from twitterStreamer import secret
from twitterStreamer.websciAE import StreamListener


config_file = 'twitterStreamer/config.ini'


def setup_logger(logfile):
    # setup logging
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def config_mongo():
    mongo_host = ['SAMPLE_STREAM']['mongo_host']
    db_name = ['SAMPLE_STREAM']['db']
    collection = 'test' # ['SAMPLE_STREAM']['collection']
    client = MongoClient(mongo_host + db_name)
    db = client.db_name

    return collection, db, mongo_host


def get_auth():
    consumer_key = secret.CONSUMER_KEY
    consumer_secret = secret.CONSUMER_SECRET
    access_token = secret.ACCESS_TOKEN
    access_token_secret = secret.ACCESS_TOKEN_SECRET
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


def collect_streaming_sample():
    config = configParser.ConfigParser()
    config.read(config_file)
    collection, db, mongo_host = config_mongo()
    time_limit = ['SAMPLE_STREAM']['time_limit']
    logfile = config['SAMPLE_STREAM']['logfile']
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = StreamListener(logger, mongo_host, time_limit, auth, db, collection, api=tweepy.API(wait_on_rate_limit=True))
    stream = Stream(auth, listener)
    stream.sample()


if __name__ == '__main__':
    collect_streaming_sample()



# stream.filter(track=KEYWORD)
# logger.info("Finished")

# logfile = '/home/paul/PycharmProjects/websciAE/docs/streamlog.log'


