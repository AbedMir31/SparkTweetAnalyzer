import socket
import pandas as pd 
import tweepy

access_token="1326362589002149888-L1HZIK8K5vYHRPfOktglTmhlSk5KsK"
access_secret="Qf8gf7dLvUrKqAssPmxqbJJ8XvlRG054oLmbJnBBBRMp8"
consumer_key="jh9Tl3HwDRGk3or5AJYdCNVMV"
consumer_secret="g8OTo6hQOXoXdhWi3eb4zRhEkfMaee765vcLIPSFjbr7sZENF4"

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print("RECEIVED DATA: ", status)
        conn.send(status)
        return True

    def on_error(self, data):
        print(data)
    

if __name__ == "__main__":
    hashtag = '#Halo'

    TCP_IP = 'localhost'
    TCP_PORT = 9001

    # create sockets
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    conn, addr = s.accept()
    
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
    myStream.filter(track=[hashtag], languages=["en"], is_async=True)