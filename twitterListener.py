import os
import time
import json
import pyspark
import pickle
import settings

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import desc
from pyspark.streaming import StreamingContext

HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))


def sampleWord(rdd):
 return rdd.sample(False,0.5,10)

def filter_tweets(tweet):
  json_tweet = json.loads(tweet)
  if 'lang' in json_tweet: # When the lang key was not present it caused issues
      if json_tweet['lang'] == 'en':
        return True # filter() requires a Boolean value
  return False
  
conf = SparkConf().setAppName("Twitter tweets listener").setMaster('local[2]')
sparkContext = SparkContext(conf=conf)
sparkContext.setLogLevel("ERROR")

sqlContext = SQLContext(sparkContext)

streamingContext = StreamingContext(sparkContext, 1)

dstream = streamingContext.socketTextStream(HOST, PORT)
json_objects = dstream.filter(lambda input: filter_tweets(input))
sampled = json_objects.transform(sampleWord)
texts = sampled.map(lambda obj: json.loads(obj)['text'])
# Print the first ten elements of each RDD generated in this DStream to the console
counter = texts.count()
counter.pprint()
#   .filter( lambda word: word.lower().startswith("#") )\
#   .map( lambda word: ( word.lower(), 1 ) )\
#   .reduceByKey( lambda a, b: a + b )\
#   .map( lambda rec: Tweet( rec[0], rec[1] ) )\
#   .pprint(10)


streamingContext.start()
streamingContext.awaitTermination()