from tweepy import Stream

import sample_stream_listener
import pipeline


def config_keyword_stream():
    config = pipeline.load_config()
    collection = config['DEFAULT']['collection']
    time_limit = int(config['KEYWORD_STREAM']['time_limit'])
    keywords = config['KEYWORD_STREAM']['keywords']
    return collection, time_limit, keywords


def collect_keyword_stream():
    mongo_host = pipeline.config_mongodb()
    collection_name, time_limit, tags = config_keyword_stream()
    logger = pipeline.setup_logger('keyword_stream')
    auth = pipeline.get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit,  auth)
    keywords_stream = Stream(auth, listener)
    return keywords_stream, tags