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

# Our filter function:
def filter_tweets(tweet):
  if tweet.has_key('lang'): # When the lang key was not present it caused issues
      if tweet['lang'] == 'en':
        return True # filter() requires a Boolean value
  return False
  
conf = SparkConf().setAppName("Twitter tweets listener").setMaster('local[2]')
sparkContext = SparkContext(conf=conf)
sparkContext.setLogLevel("ERROR")

sqlContext = SQLContext(sparkContext)

streamingContext = StreamingContext(sparkContext, 10)

dstream = streamingContext.socketTextStream(HOST, PORT)
json_objects = dstream.map(lambda input: json.loads(input)['text'])

# Print the first ten elements of each RDD generated in this DStream to the console
json_objects.pprint()
#   .filter( lambda word: word.lower().startswith("#") )\
#   .map( lambda word: ( word.lower(), 1 ) )\
#   .reduceByKey( lambda a, b: a + b )\
#   .map( lambda rec: Tweet( rec[0], rec[1] ) )\
#   .pprint(10)


streamingContext.start()
streamingContext.awaitTermination()