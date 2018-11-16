from tweepy import Stream

from collectors import sample_stream_listener
import pipeline


def config_sample_stream(config):
    collection = config['DEFAULT']['collection']
    time_limit = int(config['SAMPLE_STREAM']['time_limit'])
    return collection, time_limit,


def collect_streaming_sample():
    config = pipeline.load_config()
    db, mongo_host = pipeline.config_mongodb()
    collection_name, time_limit = config_sample_stream(config)
    logger = pipeline.setup_logger('streaming.log')
    auth = pipeline.get_auth()
    listener = sample_stream_listener.SampleStreamListener(logger, mongo_host, collection_name, time_limit,  auth)
    sample_stream = Stream(auth, listener)
    return sample_stream











