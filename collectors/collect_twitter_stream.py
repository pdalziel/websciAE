import logging
import configparser
import os

import tweepy
from pymongo import MongoClient
from time import sleep

from tweepy import Stream
from collectors import get_tokens
from collectors import sample_stream_listener

# TODO refactor config to remove redundancy


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
    collection = config['SAMPLE_STREAM']['collection']
    time_limit = int(config['SAMPLE_STREAM']['time_limit'])
    logfile = config['SAMPLE_STREAM']['logfile']
    return db, mongo_host, collection, time_limit, logfile


def config_keyword_stream(config):
    config = load_config()
    mongo_host = config['KEYWORD_STREAM']['mongo_host']
    db = config['KEYWORD_STREAM']['db']
    collection = config['KEYWORD_STREAM']['collection']
    time_limit = int(config['KEYWORD_STREAM']['time_limit'])
    logfile = config['KEYWORD_STREAM']['logfile']
    keywords = config['KEYWORD_STREAM']['keywords']
    return db, mongo_host, collection, time_limit, logfile, keywords


def config_user_stream(config):
    config = load_config()
    mongo_host = config['USER_STREAM']['mongo_host']
    db = config['USER_STREAM']['db']
    collection = config['USER_STREAM']['collection']
    time_limit = int(config['USER_STREAM']['time_limit'])
    logfile = config['USER_STREAM']['logfile']
    user_ids = config['USER_STREAM']['user_ids']
    return db, mongo_host, collection, time_limit, logfile, user_ids


def config_location_stream(config):
    config = load_config()
    mongo_host = config['LOCATION_STREAM']['mongo_host']
    db = config['LOCATION_STREAM']['db']
    collection = config['LOCATION_STREAM']['collection']
    time_limit = int(config['LOCATION_STREAM']['time_limit'])
    logfile = config['LOCATION_STREAM']['logfile']
    location = [config['LOCATION_STREAM']['location']]
    return db, mongo_host, collection, time_limit, logfile, location


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
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit,  auth)
    sample_stream = Stream(auth, listener)
    return sample_stream


def collect_keyword_stream():
    config = load_config()
    db, mongo_host, collection_name, time_limit, logfile, tags = config_keyword_stream(config)
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit,  auth)
    keywords_stream = Stream(auth, listener)
    return keywords_stream, tags


def collect_user_stream():
    config = load_config()
    db, mongo_host, collection_name, time_limit, logfile, user_ids = config_user_stream(config)
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit, auth)
    keywords_stream = Stream(auth, listener)
    return keywords_stream, user_ids


def collect_location_stream():
    config = load_config()
    db, mongo_host, collection_name, time_limit, logfile, location = config_location_stream(config)
    logger = setup_logger(logfile)
    auth = get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit, auth)
    location_stream = Stream(auth, listener)
    return location_stream, location


if __name__ == '__main__':
    script_dir = os.path.dirname(__file__)  # <-- absolute dir the script is in
    rel_path = "docs/config.ini"
    parentDir = os.path.dirname(script_dir)
    config_file = os.path.join(parentDir, rel_path)

    stream = collect_streaming_sample()
    stream.sample(languages=['en'], async=True)
    keyword_stream, tracking_tags = collect_keyword_stream()
    keyword_stream.filter(track=['#brexit'], async=True)
    user_stream, user_ids = collect_user_stream()
    user_stream.filter(follow=['813286', '25073877'], async=True)

    location_stream, location = collect_location_stream()
    location_stream.filter(locations=[-4.516, 55.7225, -3.975, 55.978], async=True)
    # TODO fix thread lock







