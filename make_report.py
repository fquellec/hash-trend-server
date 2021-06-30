from searchTweets import searchTweets
import pymongo
import credentials
import pandas as pd

MIN_NB_TWEETS = 10
MAX_NB_TWEETS = 20000

def make_report(query):
	client = pymongo.MongoClient(credentials.MONGO_URI)
	db = client.hashTrend
	queries = db.queries

	# Update job status on db
	myquery = { "query": query }
	newvalues = { "$set": { "status": "Start fetching tweets", } }
	queries.update_one(myquery, newvalues)

	df, ok = searchTweets(query, MIN_NB_TWEETS, MAX_NB_TWEETS)
	if not ok:
		return 204

	df.date = pd.to_datetime(df.date)

	# Compute volume of tweets by hours
	volumes = df.resample('h', on='date').count()[['tweet_id']].reset_index().sort_values(by='date')
	volumes = volumes.rename(columns={'date':'name', 'tweet_id':'value'}).to_dict(orient="records")

	# Generate json report
	result = {
		'generalInfos': {
			'nbTweets': int(df.shape[0]),
			'nbLikes':int(df.likes.sum()),
			'nbRetweets': int(df.retweets.sum()),
			'nbUsers': len(df.user_name.unique()),
		},
		'volumes': volumes,
	}

	# Update db
	client = pymongo.MongoClient(credentials.MONGO_URI)
	db = client.hashTrend
	queries = db.queries
	myquery = { "query": query }
	newvalues = { "$set": { "result": result, "code": 200, "status": "Done"} }
	queries.update_one(myquery, newvalues)
	return 200
