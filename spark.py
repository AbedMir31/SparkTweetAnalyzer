from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import googlemaps
import re, string
import random
import json
from textblob import TextBlob
from elasticsearch import Elasticsearch

TCP_IP = 'localhost'
TCP_PORT = 9001
def processTweet(tweet):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}]) # 9200 by Default

    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search         

    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        full_location = tweetData[0]
        lat = tweetData[1]
        lng = tweetData[2]
        rawLocation = [lat, lng]
        text = tweetData[3]

        # (i) Apply Sentiment analysis in "text"
        blob = TextBlob(text)
        if blob.sentiment.polarity < 0:
            sentiment = "Negative"
        elif blob.sentiment.polarity == 0:
            sentiment = "Neutral"
        else:
            sentiment = "Positive"

	 # (ii) Get geolocaton (state, country, lat, lon, etc...) from rawLocation
        
        print("\n\n=========================\ntweet: ", text, "\nlocation: ", full_location, "\nSentiment: ", sentiment, "\n=========================\n\n")

     # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        with open("logger.json", "a") as data:
            pack_tweet = {
                "text":text,
                "location":full_location,
                "geocode":rawLocation,
                "sentiment":sentiment
            }
            data.write(json.dumps(pack_tweet))
            data.write('\n')
            data.close()

        # Indexing, changed for heat map implementation
        esDoc = {
                "text":text,
                "location":full_location,
                "geocode":{rawLocation,{"type": "geo_point"}},
                "sentiment":sentiment
            }
        es.index(index = 'tweet-data', body=esDoc)


if __name__ == "__main__":
    # Pyspark
    # create spark configuration
    conf = SparkConf()
    conf.setAppName('twitterApp')
    conf.setMaster('local[2]')

    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("FATAL")

    # create the Streaming Context from spark context with interval size 4 seconds
    ssc = StreamingContext(sc, 4)
    ssc.checkpoint("checkpoint_TwitterApp")

    # read data from port 900
    dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
    dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))

    ssc.start()
    ssc.awaitTermination()
