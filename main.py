import logging
import configparser


import tweepy
from pymongo import MongoClient
from tweepy import Stream
from collectors import get_tokens
from collectors import sample_stream_listener


def setup_logger(logfile):
    # setup logging
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def config_mongo(config):

    mongo_host = config['SAMPLE_STREAM']['mongo_host']
    db = config['SAMPLE_STREAM']['db']
    return db, mongo_host


def get_auth():
    secrets_dict = get_tokens()
    consumer_key = secrets_dict.get("consumer_key")
    consumer_secret = secrets_dict.get("consumer_secret")
    access_token = secrets_dict.get("access_token")
    access_token_secret = secrets_dict.get("access_token_secret")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


def collect_streaming_sample():
    config = configparser.ConfigParser()
    config.read(config_file)
    db, mongo_host = config_mongo(config)
    time_limit = int(config['SAMPLE_STREAM']['time_limit'])
    logfile = config['SAMPLE_STREAM']['logfile']
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, time_limit, auth)
    stream = Stream(auth, listener)
    return stream


def collect_filtered_stream(track_tags, users):
    pass
    

if __name__ == '__main__':

    config_file = "/home/paul/PycharmProjects/websciAE/docs/config.ini"
    stream = collect_streaming_sample()
    #stream.
    stream.filter(follow=['44196397'])




# stream.filter(track=KEYWORD)
# logger.info("Finished")

# logfile = '/home/paul/PycharmProjects/websciAE/docs/streamlog.log'


