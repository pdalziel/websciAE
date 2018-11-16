from tweepy import Stream

import sample_stream_listener
import pipeline


def config_location_stream():
    config = pipeline.load_config()
    collection = config['DEFAULT']['collection']
    time_limit = int(config['LOCATION_STREAM']['time_limit'])
    location = [config['LOCATION_STREAM']['location']]
    return collection, time_limit, location


def collect_location_stream():
    mongo_host = pipeline.config_mongodb()
    collection_name, time_limit, location = config_location_stream()
    logger = pipeline.setup_logger('location_stream')
    auth = pipeline.get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit, auth)
    location_stream = Stream(auth, listener)
    return location_stream, location