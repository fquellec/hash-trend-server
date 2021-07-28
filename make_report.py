from searchTweets import searchTweets
import pymongo
import credentials
import pandas as pd
import numpy as np
from textblob import TextBlob
import re
import ast # Convert string list to list
import json
import networkx as nx
from networkx.readwrite import json_graph
import return_codes
from littleballoffur import PageRankBasedSampler

MIN_NB_TWEETS = 100
MAX_NB_TWEETS = 20000

def make_report(query):
        client = pymongo.MongoClient(credentials.MONGO_URI)
        db = client.hashTrend
        queries = db.queries

        # Update job status on db
        myquery = { "query": query }
        newvalues = { "$set": { "status": "Start fetching tweets", "code": return_codes.PROCESSING} }
        queries.update_one(myquery, newvalues)

        print("query: ", query)

        try:
            df, ok = searchTweets(query, MIN_NB_TWEETS, MAX_NB_TWEETS)
        except Exception as e:
            myquery = { "query": query }
            newvalues = { "$set": { "status": str(e), "code": return_codes.ERROR} }
            queries.update_one(myquery, newvalues)
            return return_codes.ERROR

        df = pd.read_csv("results/#test.csv", dtype=object, na_filter=False)

        if df is None:
            return return_codes.NO_CONTENT

        df.date = pd.to_datetime(df.date)
        df.followers = pd.to_numeric(df.followers)
        df.likes = pd.to_numeric(df.likes)
        df.retweets = pd.to_numeric(df.retweets)

        print("search done", ok)
        print("total", df.shape[0])

        myquery = { "query": query }
        newvalues = { "$set": { "status": "Generating report...", "code": return_codes.PROCESSING} }
        queries.update_one(myquery, newvalues)

        # Compute volume of tweets by hours
        volumes = df.resample('h', on='date').count()[['tweet_id']].reset_index().sort_values(by='date')
        volumes = volumes.rename(columns={'date':'name', 'tweet_id':'value'}).to_dict(orient="records")

        # Top tweet
        top_tweet = df[(df.retweet_from_user_id == "") & (df.reply_to_tweet_id == "")].sort_values(by=['retweets', 'likes', 'followers'], ascending=False).head(10).tweet_id.to_list()
        top_tweet

        # Top Actors
        top_actors = df.groupby('user_name').agg({
            'followers': 'max',
            'likes':'sum',
            'retweets':'sum',
            'tweet_id':'count'
            })
        top_actors = top_actors.rename(columns={'tweet_id':'nb_tweets'}).sort_values(by=['nb_tweets', 'followers', 'retweets'], ascending=False).reset_index()
        top_actors = top_actors.to_dict(orient="records")
        top_actors

        # Hashtags
        hashtags_freq = []

        for i, row in df.iterrows():
            hashtags = json.loads(row['hashtags'].replace("'","\""))
            for h in hashtags:
                hashtag = h['text'].lower()
                if hashtag != query.lower().replace("#","",1):
                    hashtags_freq.append({'date':row['date'], 'hashtag':hashtag})

        hashtags_freq_df = pd.DataFrame(hashtags_freq)
        hashtags_top = hashtags_freq_df.groupby('hashtag').count().reset_index().rename(columns={'hashtag':'name', 'date':'value'}).sort_values(by='value', ascending=False).head(10).to_dict(orient="records")
        hashtags_freq_df.date = pd.to_datetime(hashtags_freq_df.date)
        hashtags_freq_df = hashtags_freq_df.set_index("date").groupby('hashtag').resample('1D').count().rename(columns={'hashtag':'count'}).reset_index()
        hashtags_cum_sum = hashtags_freq_df.set_index('date').groupby('hashtag').cumsum().reset_index()
        hashtags_cum_sum['hashtag'] = hashtags_freq_df.hashtag
        hashtags_cum_sum = hashtags_cum_sum.to_dict(orient="records")

        # Sentiment
        def clean_tweet(tweet):
            return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

        def get_tweet_sentiment(text):
            analysis = TextBlob(clean_tweet(text))
            # set sentiment
            if analysis.sentiment.polarity > 0:
                return "Positive"
            elif analysis.sentiment.polarity == 0:
                return "Neutral"
            else:
                return "Negative"

        df['sentiment'] = df.text.apply(lambda t: get_tweet_sentiment(t))
        sentiments = df.groupby('sentiment').count().tweet_id.to_dict()

        # Graph
        graph = nx.DiGraph()

        def addConnection(graph, from_id, from_username, to_id, to_username):
            if graph.has_node(from_id):
                graph.nodes[from_id]['interaction'] += 1
            else:
                graph.add_node(from_id, interaction=1, name=from_username, nb_tweets=0)

            # Add or update node interaction
            if graph.has_node(to_id):
                graph.nodes[to_id]['interaction'] += 1
            else:
                graph.add_node(to_id, interaction=1, name=to_username, nb_tweets=0)

            # Add or update edge user - interaction
            if graph.has_edge(from_id, to_id):
                graph[from_id][to_id]['weight'] += 1
            else:
                graph.add_edge(from_id, to_id, weight=1)

        for i, tweet in df.iterrows():
            print(f"{i}/{df.shape[0]}\r", end="")
            interactions = set()

            # Add interaction if the tweet is a reply to another user
            #if tweet['reply_to_user_id'] != "":
            #    interactions.add((tweet['reply_to_user_id'], tweet['reply_to_username']))

            # Add interaction if its a simple retweet
            if tweet['retweet_from_user_id'] != "":
                interactions.add((tweet['retweet_from_user_id'], tweet['retweet_from_username']))

            # Add interaction if its a quoted retweet
            #if tweet['quote_from_user_id'] != "":
            #    interactions.add((tweet['quote_from_user_id'], tweet['quote_from_username']))

            # Add interaction for each mention of another user in the tweet
            #user_ids = ast.literal_eval(tweet['user_mentions_ids'])
            #user_names = ast.literal_eval(tweet['user_mentions_names'])
            #for mention in zip(user_ids,user_names):
            #    interactions.add((mention[0], mention[1]))

            # Discard interaction toward itself
            interactions.discard((tweet['user_id'], tweet['user_name']))

            # Update number of tweets by author
            if graph.has_node(tweet['user_id']):
                graph.nodes[tweet['user_id']]['nb_tweets'] += 1
                graph.nodes[tweet['user_id']]['followers'] = tweet['followers']
                graph.nodes[tweet['user_id']]['following'] = tweet['following']
            else:
                graph.add_node(tweet['user_id'],
                    nb_tweets=1,
                    interaction=0,
                    name=tweet['user_name'],
                    followers=tweet['followers'],
                    following=tweet['following'])

            # Add interactions to the graph
            for interaction in interactions:
                addConnection(graph, tweet['user_id'], tweet['user_name'], interaction[0], interaction[1])


        print(f"There are {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges present in the Graph")

        degrees = [val for (node, val) in graph.degree()]

        print(f"The maximum degree of the Graph is {np.max(degrees)}")
        print(f"The minimum degree of the Graph is {np.min(degrees)}")

        if nx.is_connected(graph.to_undirected()):
            print("The graph is connected")
        else:
            print("The graph is not connected")

        print(f"There are {nx.number_connected_components(graph.to_undirected())} connected components in the Graph")

        def connected_component_subgraphs(G):
            for c in nx.connected_components(G):
                yield G.subgraph(c)

        largest_subgraph = max(connected_component_subgraphs(graph.to_undirected()), key=len)
        
        number_of_nodes = largest_subgraph.number_of_nodes()
        number_of_edges = largest_subgraph.number_of_edges()
        
        print(f"There are {number_of_nodes} nodes and {number_of_edges} \
        edges present in the largest component of the Graph")
        
        # Sample the graph with maximum 4000 nodes
        number_of_nodes = 4000
        sampler = PageRankBasedSampler(number_of_nodes = number_of_nodes)
        largest_subgraph = sampler.sample(largest_subgraph)

        graph = json_graph.node_link_data(largest_subgraph.to_undirected())
        # Location
        #geolocator = Nominatim(user_agent="hash-trend app")    
        #geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0)
        #locations = df[df.location != ""].groupby('location').count().reset_index()[['location', 'tweet_id']]
        #locations['geocode'] = df['location'].apply(partial(geocode, exactly_one=True, addressdetails=True))
        #locations[locations.geocode.notnull()].tweet_id.sum()
        #locations['country_code'] = df['geocode'].apply(lambda loc: loc.raw['address']['country_code'] if loc else None)

        # Generate json report
        result = {
                'generalInfos': {
                        'nbTweets': int(df.shape[0]),
                        'nbLikes':int(df.likes.sum()),
                        'nbRetweets': int(df.retweets.sum()),
                        'nbUsers': len(df.user_name.unique()),
                },
                'volumes': volumes,
                'top_tweet': top_tweet,
                'top_actors': top_actors,
                'sentiment': sentiments,
                'hahstags_cum_sum': hashtags_cum_sum,
                'hahstags_top_10': hashtags_top,
                'graph': graph,
                #'location': None,
        }
        result
        print("report generated")

        # Update db
        client = pymongo.MongoClient(credentials.MONGO_URI)
        db = client.hashTrend
        queries = db.queries
        myquery = { "query": query }
        if not ok:
            code = return_codes.PARTIAL_RESULT
            status = "Not all tweets have been retrieved"
        else:
            code = return_codes.FINISH
            status = "Done"

        newvalues = { "$set": { "result": result, "code": code, "status": status} }
        queries.update_one(myquery, newvalues)

        print("report updated")

        return code
