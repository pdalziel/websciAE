# TODO Count amount of data collected
# TODO Specify amount of geo-tagged data
# TODO Count redundant data present in the collection (same tweets)
# TODO Count the re-tweets and quotes


from utils.connect import Connect


def count_total_collected_data(collection=None):
    # TODO get total

    conn = Connect.get_connection()
    db = conn.twitterdb
    if collection is None:
        collection = ''
        collections = db.collection_names()
        for col in collections:
            print(col)
            print(db[col].count())

    return db[collection].count()


def count_total_redundant():
    # TODO get redundant
    pass


def count_geo_tagged():
    # TODO get geo tagged
    pass


def count_retweets():
    # TODO get retweets
    pass


def count_quotes():
# TODO get quotes
    pass

