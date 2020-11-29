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

es = Elasticsearch()

def processTweet(data):

         # Here, you should implement:
         # (i) Sentiment analysis,
         # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
         # (iii) Index the data using Elastic Search      
 
         dict_data = json.loads(data)
         tweet = TextBlob(dict_data["text"])
        
         if tweet.sentiment.polarity < 0:
            sentiment = "negative"
         elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
         else:
            sentiment = "positive"

         es.index(index="sentiment",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       "sentiment": sentiment})

         print("Tweet: ", tweet)
         print("Sentiment: ", sentiment)
         
         # (i) Apply Sentiment analysis in "text"


	 # (ii) Get geolocaton (state, country, lat, lon, etc...) from rawLocation
        


         # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 


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
