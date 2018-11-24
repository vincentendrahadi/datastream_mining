import os
import json
import socket
import tweepy
import pickle
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import settings

from signal import signal, SIGPIPE, SIG_DFL

# Stop program if sigpipe detected
signal(SIGPIPE, SIG_DFL)

CONSUMER_KEY = os.getenv("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("TWITTER_ACCESS_SECRET")

class TwitterListener(StreamListener):
  def __init__(self, conn):
    super().__init__()
    self.conn = conn

  def on_data(self, data):
    try:
      data = data.replace(r'\n', '')
      json_tweet = json.loads(data)
      if(json_tweet["text"]):
        # print(json_tweet)
        self.conn.send(data.encode())
    except BaseException as e:
      print("Error on_data: %s" % str(e))

  def on_error(self, status):
    if status == 420:
      print("Stream Disconnected")
      return False

def sendTwitterData(conn):
  auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

  api = tweepy.API(auth)

  stream = tweepy.Stream(auth = api.auth, listener=TwitterListener(conn))
  stream.sample()


if __name__ == "__main__":
  host = os.getenv("HOST")
  port = int(os.getenv("PORT"))

  s = socket.socket()
  s.bind((host, port))
  print("Listening on port:", str(port))

  s.listen(5)
  conn, addr = s.accept()
  print("Received request from:", str(addr))

  sendTwitterData(conn)

