import tweepy
import socket
import re #Here to preprocess the tweet for emoji removal
import preprocessor as p 
import sys
import googlemaps
import pandas as pd 


access_token="1326362589002149888-L1HZIK8K5vYHRPfOktglTmhlSk5KsK"
access_secret="Qf8gf7dLvUrKqAssPmxqbJJ8XvlRG054oLmbJnBBBRMp8"

consumer_key="jh9Tl3HwDRGk3or5AJYdCNVMV"
consumer_secret="g8OTo6hQOXoXdhWi3eb4zRhEkfMaee765vcLIPSFjbr7sZENF4"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

TCP_IP = 'localhost'
TCP_PORT = 9001

def rehelper(text):
    #cleaning the pattern
    cleaned = re.compile(pattern = "["
        u"\U0001F300-\U0001F5FF"
        u"\U0001F680-\U0001F6FF" 
        u"\U0001F1E0-\U0001F1FF"
                                    "]+", flags=re.UNICODE)
    return cleaned.sub(r'',text)

def preprocessing(tweet):
    
    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
    #here using either re or preprocessor
    p.clean(tweet)
    hold = rehelper(tweet)
    tweet = hold.replace('#', '')
    return tweet

def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ""
    location = ""
    location = status.user.location
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        googleholder=googlemaps.Client(key ="AIzaSyCJdLk_x5tkFvAHNAeOB5-VGTP1MCEMiL0")
        location, tweet = getTweet(status)
        if location != None:
            result = googleholder.geocode(location)
            if result:
                lat =result[0]['geometry']['location']
                local_latlong=str(lat['lng']) + str(lat['lat'])
            else:
                local_latlong=""
        else:
            local_latlong=""
        if (location != None and tweet != None):
            tweetLocation = local_latlong + "::" + tweet+"\n"
            print(status.text)
            conn.send(tweetLocation.encode('ascii', 'ignore'))
        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)
if __name__ == "__main__":
    
    hashtag = '#Minecraft'

    # create sockets
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    conn, addr = s.accept()

    myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
    myStream.filter(track=[hashtag], languages=["en"], is_async=True)