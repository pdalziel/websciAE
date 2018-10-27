import logging
import configparser


import tweepy
from pymongo import MongoClient
from tweepy import Stream
from collectors import get_tokens
from collectors import sample_stream_listener

# TODO refactor config to redundancy


def load_config():
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def setup_logger(logfile):
    # setup logging
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def config_sample_stream(config):
    mongo_host = config['SAMPLE_STREAM']['mongo_host']
    db = config['SAMPLE_STREAM']['db']
    collection = ['SAMPLE_STREAM']['collection']
    time_limit = int(config['SAMPLE_STREAM']['time_limit'])
    logfile = config['SAMPLE_STREAM']['logfile']
    return db, mongo_host, collection, time_limit, logfile


def config_keyword_stream(config):
    config = load_config()
    mongo_host = config['KEYWORD_STREAM']['mongo_host']
    db = config['KEYWORD_STREAM']['db']
    collection = ['KEYWORD_STREAM']['collection']
    time_limit = int(config['KEYWORD_STREAM']['time_limit'])
    logfile = config['KEYWORD_STREAM']['logfile']
    keywords = config['KEYWORD_STREAM']['keywords']
    return db, mongo_host, collection, time_limit, logfile, keywords


def config_user_stream(config):
    config = load_config()
    mongo_host = config['USER_STREAM']['mongo_host']
    db = config['USER_STREAM']['db']
    collection = ['USER_STREAM']['collection']
    time_limit = int(config['USER_STREAM']['time_limit'])
    logfile = config['USER_STREAM']['logfile']
    user_ids = config['USER_STREAM']['user_ids']
    return db, mongo_host, collection, time_limit, logfile, user_ids


def get_auth():
    secrets_dict = get_tokens.load_access_token()
    consumer_key = secrets_dict.get("consumer_key")
    consumer_secret = secrets_dict.get("consumer_secret")
    access_token = secrets_dict.get("access_token")
    access_token_secret = secrets_dict.get("access_token_secret")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth


def collect_streaming_sample():
    config = load_config()
    db, mongo_host, collection_name, time_limit, logfile = config_sample_stream(config)
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, time_limit, collection_name, auth)
    sample_stream = Stream(auth, listener)
    return sample_stream


def collect_keyword_stream():
    config = load_config()
    db, mongo_host, collection_name, time_limit, logfile, tags = config_keyword_stream(config)
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, time_limit, collection_name, auth)
    keywords_stream = Stream(auth, listener)
    return keywords_stream, tags


def collect_user_stream():
    config = load_config()
    db, mongo_host, collection_name, time_limit, logfile, user_ids = config_user_stream(config)
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, time_limit, collection_name, auth)
    keywords_stream = Stream(auth, listener)
    return keywords_stream, user_ids


if __name__ == '__main__':
    config_file = "/home/paul/PycharmProjects/websciAE/docs/config.ini"
    stream = collect_streaming_sample()
    keyword_stream, tracking_tags = collect_keyword_stream()
    keyword_stream.filter(track=tracking_tags,  languages='en')
    user_stream, user_ids = collect_user_stream()
    user_stream.filter(follow=user_ids, languages='en')
    stream.sample(language='en')



# stream.filter(track=KEYWORD)
# logger.info("Finished")

# logfile = '/home/paul/PycharmProjects/websciAE/docs/streamlog.log'


