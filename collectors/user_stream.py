from tweepy import Stream

import sample_stream_listener
import pipeline


def config_user_stream():
    config = pipeline.load_config()
    collection = config['DEFAULT']['collection']
    time_limit = int(config['USER_STREAM']['time_limit'])
    user_ids = config['USER_STREAM']['user_ids']
    return collection, time_limit, user_ids


def collect_user_stream():
    mongo_host = pipeline.config_mongodb()
    collection_name, time_limit, user_ids = config_user_stream()
    logger = pipeline.setup_logger('user_stream')
    auth = pipeline.get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit, auth)
    keywords_stream = Stream(auth, listener)
    return keywords_stream, user_ids