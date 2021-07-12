import tweepy
import os
import csv
import credentials
import pandas as pd
import pymongo
import gc

def saveTweet(row, filename, perm='a'):
    with open(filename, perm) as f:
        writer = csv.writer(f)
        writer.writerow(row)

def searchTweets(query, min_tweets, max_tweets):
    # connect to db
    client = pymongo.MongoClient(credentials.MONGO_URI)
    db = client.hashTrend
    queries = db.queries

    # Create target Directory if don't exist
    if not os.path.exists('results'):
        os.mkdir('results')

    FILENAME = os.path.join('results', query + ".csv")

    # Fields we wants to retrieve from tweets
    header = [
              'tweet_id',
              'user_id',
              'user_name',
              'followers',
              'following',
              'likes',
              'retweets',
              'date',
              'reply_to_tweet_id',
              'reply_to_user_id',
              'reply_to_username',
              'user_mentions_ids',
              'user_mentions_names',
              'text',
              'retweet_from_user_id',
              'retweet_from_username',
              'retweet_from_tweet_id',
              'quote_from_tweet_id',
              'quote_from_user_id',
              'quote_from_username',
              'location',
              'location_full',
              'lang',
              'hashtags'
              ]

    saveTweet(header, FILENAME, 'w')

    # Set access tokens for Twitter Api, use Application only Auth instead of the Access Token Auth since it has higher limits rates.
    auth = tweepy.AppAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    if (not api):
        print ("Can't Authenticate")
        sys.exit(-1)


    counter = 0
    for tweets in tweepy.Cursor(api.search, q=query, count=100, tweet_mode="extended").pages():
        for tweet in tweets:
            tweet_id = tweet.id_str
            user_id = tweet.user.id_str
            lang = tweet.lang
            location = tweet.user.location
            try:
                location_full = tweet.user.derived.locations
            except:
                location_full = None

            user_name = tweet.user.screen_name
            followers = tweet.user.followers_count
            following = tweet.user.friends_count
            likes = tweet.favorite_count
            retweets = tweet.retweet_count
            date = tweet.created_at
            reply_to_tweet_id = tweet.in_reply_to_status_id_str
            reply_to_user_id = tweet.in_reply_to_user_id_str
            reply_to_username = tweet.in_reply_to_screen_name
            user_mentions_ids = [mention['id_str'] for mention in tweet.entities['user_mentions']]
            user_mentions_names = [mention['screen_name'] for mention in tweet.entities['user_mentions']]
            if hasattr(tweet, "full_text"):
                text = tweet.full_text
            else:
                if tweet.truncated:
                    text = tweet.extended_tweet['full_text']
                else:
                    text = tweet.text
            hashtags = tweet.entities['hashtags']
            retweet_from_user_id = None
            retweet_from_username = None
            retweet_from_tweet_id = None

            if hasattr(tweet, "retweeted_status"):
                retweet_from_user_id = tweet.retweeted_status.user.id_str
                retweet_from_username = tweet.retweeted_status.user.screen_name
                retweet_from_tweet_id = tweet.retweeted_status.id_str
                if hasattr(tweet.retweeted_status, "full_text"):
                    text = tweet.retweeted_status.full_text
                else:
                    if tweet.retweeted_status.truncated:
                        text = tweet.retweeted_status.extended_tweet['full_text']
                    else:
                        text = tweet.retweeted_status.text

            quote_from_tweet_id = None
            quote_from_user_id = None
            quote_from_username = None
            if hasattr(tweet, "quoted_status"):
                quote_from_tweet_id = tweet.quoted_status.id_str
                quote_from_user_id = tweet.quoted_status.user.id_str
                quote_from_username = tweet.quoted_status.user.screen_name

            row = [
                   tweet_id,
                   user_id,
                   user_name,
                   followers,
                   following,
                   likes,
                   retweets,
                   date,
                   reply_to_tweet_id,
                   reply_to_user_id,
                   reply_to_username,
                   user_mentions_ids,
                   user_mentions_names,
                   text,
                   retweet_from_user_id,
                   retweet_from_username,
                   retweet_from_tweet_id,
                   quote_from_tweet_id,
                   quote_from_user_id,
                   quote_from_username,
                   location,
                   location_full,
                   lang,
                   hashtags
                   ]

            saveTweet(row, FILENAME)

            counter += 1
            print(f"tweets fetched: {counter}\r", end="")  

            if counter > max_tweets:
                # Update db
                myquery = { "query": query }
                newvalues = { "$set": { "status": "Too much tweets retrieved", "code": 204, } }
                queries.update_one(myquery, newvalues)
                return None, False

        # Update db
        myquery = { "query": query }
        newvalues = { "$set": { "status": f"{counter} tweets retrieved", "code": 202, } }
        queries.update_one(myquery, newvalues)

    #print("Total fetched : " + str(counter))
    df = pd.read_csv(FILENAME, dtype=object, na_filter=False)
    if df.shape[0] < min_tweets:
        # Update db
        myquery = { "query": query }
        newvalues = { "$set": { "status": "Not enough tweets retrieved", "code": 204, } }
        queries.update_one(myquery, newvalues)
        return None, False

    return df, True
