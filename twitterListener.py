import os
import time
import json
import pyspark
import pickle
import settings
import matplotlib.pyplot as plt

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from bs4 import BeautifulSoup

HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
ANDROID_SOURCE = "Twitter for Android"
IPHONE_SOURCE = "Twitter for iPhone"

def iterate_rdd(rdd, **kwargs): 
  rdd.foreach(lambda record: kwargs["android"].add(record[1]) \
    if record[0] == ANDROID_SOURCE else kwargs["iphone"].add(record[1]))

def samping_function(rdd):
 return rdd.sample(False,0.5,10)

def get_source_from_tweet(tweet):
  json_tweet = json.loads(tweet)
  if 'source' in json_tweet:
    source = json_tweet['source']
    parsed_html = BeautifulSoup(source, 'html.parser')
    source = parsed_html.a.string
    return source

def filter_tweets_source(tweet):
  json_tweet = json.loads(tweet)
  if 'source' in json_tweet:
    source = json_tweet['source']
    parsed_html = BeautifulSoup(source, 'html.parser')
    source = parsed_html.a.string
    if source == 'Twitter for Android' or source == 'Twitter for iPhone':
      return True # filter() requires a Boolean value
  return False

def filter_retweeted(tweet):
  json_tweet = json.loads(tweet)
  if 'retweet_count' in json_tweet:
    text = json_tweet["text"]
    RT_text = text[:2]
    if json_tweet['retweet_count'] > 0 or RT_text == "RT":
      return True
  return False

def print_result(**kwargs):
  print("---------------------", kwargs["source_type"], "--------------")
  print("All", kwargs["all"].value)
  print("Retweeted", kwargs["retweeted"].value)
  
conf = SparkConf().setAppName("Twitter tweets listener").setMaster('local[2]')
sparkContext = SparkContext(conf=conf)
sparkContext.setLogLevel("ERROR")

streamingContext = StreamingContext(sparkContext, 1)

android_count = sparkContext.accumulator(0)
iphone_count = sparkContext.accumulator(0)

android_retweeted_count = sparkContext.accumulator(0)
iphone_retweeted_count = sparkContext.accumulator(0)

dstream = streamingContext.socketTextStream(HOST, PORT)

sampled = dstream.transform(samping_function)
json_objects = sampled.filter(lambda input: filter_tweets_source(input))
filtered = json_objects.filter(lambda input: filter_retweeted(input))

#For all tweets
map_source = json_objects.map(lambda input: (get_source_from_tweet(input), 1))
source_counts = map_source.reduceByKey(lambda x, y: x + y)

# For retweeted tweet
map_source = filtered.map(lambda input: (get_source_from_tweet(input), 1))
retweeted_counts = map_source.reduceByKey(lambda x, y: x + y)

source_counts.foreachRDD(lambda rdd: iterate_rdd(rdd, android=android_count, iphone=iphone_count))
retweeted_counts.foreachRDD(lambda rdd: iterate_rdd(rdd, android=android_retweeted_count, \
  iphone=iphone_retweeted_count, is_retweeted=True))

streamingContext.start()
streamingContext.awaitTerminationOrTimeout(10)

android_non_retweet = android_count.value - android_retweeted_count.value
iphone_non_retweet = iphone_count.value - iphone_retweeted_count.value

labels =["Not Retweeted", "Retweeted"]

android_sizes = [android_non_retweet, android_retweeted_count.value]
iphone_sizes = [iphone_non_retweet, iphone_retweeted_count.value]

fig1, (ax1,ax2) = plt.subplots(1,2)
ax1.pie(android_sizes,  labels=labels, shadow=True, autopct='%1.0f%%')
ax1.set(aspect="equal", title=ANDROID_SOURCE)

ax2.pie(iphone_sizes,  labels=labels, shadow=True, autopct='%1.0f%%')
ax2.set(aspect="equal", title=IPHONE_SOURCE) 
plt.show()




