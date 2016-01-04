from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import random
import config

import requests
import threading
import Queue
import time
import sys
import json
import re
#import itunes
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Twitter rate limit is 15 requests in 15 minutes
# so the batch interval size could be sixty seconds,
# But let's keep it one eighty seconds to discount disconnects
BATCH_INTERVAL = 120
BLOCK_SIZE = 1500


def tokens(song_artist):
    '''

    :param song_artist: text
    :return: cleaned text
    '''

    #Removing the special character URL @ and hashtags from the tweet
    special_chars = '\\\'\"\$\?\-'

    clean_data = re.sub(r"#\S+", "", song_artist)
    clean_data  = ''.join(e for e in re.escape(clean_data ) if e not in special_chars).strip()
    clean_data  = re.sub(r"http\S+", "", clean_data )
    clean_data  = re.sub(r"@\S+", "", clean_data )
    clean_data  = clean_data.strip()
    if clean_data:
        return clean_data

    else:
        return ""


def clean(matcher_object):
    '''

    :param matcher_object: matcher object of regex
    :return: song and artist
    '''
    #extract song and artist from the tweet based on regex matcher objects
    song = matcher_object.group('song')
    song = tokens(song)
    artist = matcher_object.group('artist')
    artist = tokens(artist)
    return song, artist


def preprocess(tweet_text):
    '''

    :param tweet_text: String
    :return: cleaned text
    '''

    # extracting the song and artist info from the tweet and cleaning the data
    regex1 = '(?<=#nowplaying)(?P<song>.*.)(?=by)by(?P<artist>.*?)from'
    regex2 = '(?<=#nowplaying)(?P<song>.*.)(?=-)-(?P<artist>.*.)'
    regex3 = '(?<=#nowplaying)(?P<song>.*.)(?=by)by(?P<artist>.*.)'
    regex4 = '(?<=)(?P<song>.*.)(?=-)-(?P<artist>.*.)(?=#nowplaying)'
    pattern1 = re.compile(regex1, re.IGNORECASE)
    pattern2 = re.compile(regex2, re.IGNORECASE)
    pattern3 = re.compile(regex3, re.IGNORECASE)
    pattern4 = re.compile(regex4, re.IGNORECASE)
    match1 = re.search(pattern1, tweet_text)
    match2 = re.search(pattern2, tweet_text)
    match3 = re.search(pattern3, tweet_text)
    match4 = re.search(pattern4, tweet_text)

    song, artist = '', ''
    if match1:
        song, artist = clean(match1)
        return song, artist

    elif match2:
        song, artist = clean(match2)
        return song, artist

    elif match3:
        song, artist = clean(match3)
        return song, artist
    elif match4:
        song, artist = clean(match4)
        return song, artist
    else:
        return song, artist

def process(save_data_path, sqlContext, q):
    '''
    Process tweet, including extraction of song information out of a tweet
    @param  tweet:  RDD
    '''
    print "\n\nMETHOD: Process\nLoop:\n"

    schema = StructType([
    StructField('raw_tweet', StringType(), False),StructField('processed_tweet', StringType(), False),StructField('song', StringType(), False),StructField('artist', StringType(), False),
    StructField('tweet_time', IntegerType(), False)
    ])

    while True:
        if not q.empty():
            tweet = q.get()
            if tweet:
                print '\nQueue Element: START # \n'

                # process - clean twitter data and saves the data in parquet format
                tweet = tweet.map(lambda tweet_data: (tweet_data[0],tokens(tweet_data[0]),preprocess(tweet_data[0]),tweet_data[1])).map(lambda (raw_tweet,tweet,(song,artist),tweet_time):(raw_tweet,tweet,song,artist,tweet_time))
                print tweet.take(10)
                # converting the processed tweet into dataframe and store as Parquet file
                df_processed_tweet = sqlContext.createDataFrame(tweet,schema).coalesce(1)

                df_processed_tweet.write.mode('append').parquet(save_data_path)

                print '\nQueue Element: END # \n'                
            

def transformFunc(t, rdd):
    '''
    Convert blank RDD to RDD of tweets
    @param t:   datetime
        Current time
    @param rdd: rdd
        Current rdd we're mapping to
    '''    
    return rdd.flatMap(lambda x: twitter_stream())


def spark_stream(sc, ssc, q):
    '''
    Establish queued spark stream.
    
    References:
    1. Unit test at https://github.com/databricks/spark-perf/blob/master/pyspark-tests/streaming_tests.py
    2. Twitter Civil Unrest Analysis with Apache Spark http://will-farmer.com/twitter-civil-unrest-analysis-with-apache-spark.html
    '''
    
    # Setup Stream
    rdd = ssc.sparkContext.parallelize([0])
    stream = ssc.queueStream([], default=rdd)

    stream = stream.transform(transformFunc)

    stream.foreachRDD(lambda t, rdd: q.put(rdd))

    
    # Run!
    ssc.start()
    ssc.awaitTermination()


def twitter_stream():
    response  = requests.post(config.url, auth=config.auth, stream=True, data=config.payload)
    print('\n\n## RESPONSE: ', response.status_code) # 200 <OK>
    
    # if response is not OK, freeze for 3 minutes at least
    if response.status_code != 200:
        print "Probably rate limited, sleep for 3 minutes: "#   +strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        time.sleep(BATCH_INTERVAL)
        yield None
    else:
        count = 0
        for line in response.iter_lines():  # Iterate over streaming tweets
            if (line and line != ''):
                try:
                    post     = json.loads(line.decode('utf-8'))

                    t = int(post['timestamp_ms'])
                    t= int(t/1000)

                    contents = [post['text'], t] # , post['coordinates'], post['place']

                    count   += 1
                    yield contents

                    if count > BLOCK_SIZE: break
                    print "\n\n### COUNT: "+str(count)
                except Exception, e:
                    print('\n\n### Exception "' + str(e) +'" in Line:\n'+ line)

                    break
                
        print "\n\n### COUNT: "+str(count)

def main():

    appName = 'GroovyBear'
    conf = SparkConf().setAppName('GroovyBear')
   # conf.set("spark.app.id", 'Abhinav-Pranav-GroovyBear')

    save_data_path = sys.argv[1]

    sc = SparkContext(conf=conf)
    assert sc.version >= '1.5.1'

    sqlContext = SQLContext(sc)

    ssc = StreamingContext(sc, BATCH_INTERVAL)

    # Stream using one thread and MAYBE:
    #       1. Push processed tweets to a queue
    #       2. Access processed tweets using another thread for visualization
    threads = []
    q = Queue.Queue()
    
    # Append threads one by one
    #   the first thread should be the one that populates the Queue
    threads.append(threading.Thread(target=spark_stream, args=(sc, ssc, q)))
    threads.append(threading.Thread(target=process, args=(save_data_path, sqlContext, q,)))
    
    [t.start() for t in threads]
    
    
if __name__ == '__main__':
    sys.exit(main())