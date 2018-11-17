# TODO Count amount of data collected
# TODO Specify amount of geo-tagged data
# TODO Count redundant data present in the collection (same tweets)
# TODO Count the re-tweets and quotes


from utils.connect import Connect


def count_total_collected_data(collection=None):
    with Connect.get_connection() as conn:
        db = conn.twitterdb
        if collection is None:
            count = 0
            collection = ''
            collections = db.collection_names()
            for col in collections:
                print(col)
                print(db[col].count())
                count += db[col].count()
            return count

    return db[collection].count()


def count_total_redundant():
    # TODO get redundant =

    pass


def count_geo_tagged(collection, location=None):
    with Connect.get_connection() as conn:
        db = conn.twitterdb
        if location is None:
            q = db[collection].find(
                {
                    'user.geo_enabled': True
                })
            geo_count = q.count()
            return geo_count
        q = db[collection].find(
            {
                'geo': {
                    '$exists': True,
                    '$ne': None
                }
            })
        


def count_retweets(collection):
    with Connect.get_connection() as conn:
        db = conn.twitterdb
        q = db[collection].find(
            {
                'retweeted_status':
                    {'$exists': True}
            })
        retweets = q.count()
    return retweets


def count_quotes(collection):
    with Connect.get_connection() as conn:
        db = conn.twitterdb
        q = db[collection].find(
            {
                'is_quote_status': True
            })
        quotes = q.count()
    return quotes
