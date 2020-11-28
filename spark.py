from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import googlemaps
# from textblob import TextBlob
# from elasticsearch import Elasticsearch



TCP_IP = 'localhost'
TCP_PORT = 9001




def processTweet(tweet):

    # Here, you should implement:
    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data using Elastic Search         

    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        rawLocation = tweetData[0]
        text = tweetData[1]
        

        # (i) Apply Sentiment analysis in "text"

	# (ii) Get geolocaton (state, country, lat, lon, etc...) from rawLocation
        
        print("\n\n=========================\ntweet: ", text)


        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        



# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('twitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)


dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))


ssc.start()
ssc.awaitTermination()
