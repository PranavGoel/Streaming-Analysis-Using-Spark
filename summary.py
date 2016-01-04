__author__ = 'pranavgoel'

import sys
import time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType

def counts(sc,sqlContext,path_raw_tweet,path_processed_tweet,output_directory):
    '''

    :param input_path: input parquet files of raw tweet and processed tweet data
    :param output_path_tweet: output of calculated data points for visualisations
    :return:
    '''
    #format processed_tweet  raw_tweet|processed_tweet|artist|song|genre|tweet_time
    # raw_tweet_path = '/Users/pranavgoel/PycharmProjects/CMPT732/data_B1'

    raw_tweets = sqlContext.read.parquet(path_raw_tweet)

    processed_tweet = sqlContext.read.parquet(path_processed_tweet).cache()

    raw_tweet_count = raw_tweets.count()
    raw_tweet_count = sc.parallelize([raw_tweet_count])
    print raw_tweet_count.take(1)
    raw_tweet_count.coalesce(1).map(lambda (count): u"%s" % (count)).saveAsTextFile(output_directory + "/raw_tweet")

    processed_tweet_count = processed_tweet.count()
    processed_tweet_count = sc.parallelize([processed_tweet_count])
    print processed_tweet_count.take(1)
    processed_tweet_count.coalesce(1).map(lambda (count): u"%s" % (count)).saveAsTextFile(output_directory + "/processed_tweet")

    count_song = processed_tweet.drop('raw_tweet').drop('processed_tweet').drop('artist').drop('genre').drop('tweet_time')
    count_song = count_song.count()
    count_song = sc.parallelize([count_song])
    print count_song.take(1)
    count_song.coalesce(1).map(lambda (count): u"%s" % (count)).saveAsTextFile(output_directory + "/song")

    count_artist = processed_tweet.drop('raw_tweet').drop('processed_tweet').drop('song').drop('genre').drop('tweet_time')
    count_artist = count_artist.distinct().count()
    count_artist = sc.parallelize([count_artist])
    count_artist.coalesce(1).map(lambda (count): u"%s" % (count)).saveAsTextFile(output_directory + "/artist")
    print count_artist.take(1)

    count_genre = processed_tweet.drop('raw_tweet').drop('processed_tweet').drop('song').drop('artist').drop('tweet_time')
    count_genre = count_genre.distinct().count()
    count_genre = sc.parallelize([count_genre])
    count_genre.coalesce(1).map(lambda (count): u"%s" % (count)).saveAsTextFile(output_directory + "/genre")
    print count_genre.take(1)

    return

def get_time(tweet_time):
    '''

    :param tweet_time: time since epoch
    :return: time in DDMMMYYHHAM format
    '''

    y = time.strftime('%Y', time.gmtime(tweet_time))    # Year
    d = time.strftime('%d', time.gmtime(tweet_time))    # Day
    h = time.strftime('%H', time.gmtime(tweet_time))   # time in 22 hr format
    m = time.strftime('%b', time.gmtime(tweet_time))    # Month abbrevated

    d = d+' '+m+' '+y+'-'+h

    return(d)

def genre_date_count(sc,sqlContext,path_processed_tweet,output_directory):
    '''

    :param path_processed_tweet: directory for processed tweets
    :param output_directory: output of calculated data points for visualisations
    :return:
    '''

    #formated tweet and itunes data raw_tweet|processed_tweet|artist|song|genre|tweet_time

    processed_music = sqlContext.read.parquet(path_processed_tweet)

    processed_music = processed_music.map(lambda x: (x[2],x[3],x[4],get_time(x[5])))
    schema = StructType([
        StructField('artist', StringType(), False),
        StructField('song', StringType(), False),
        StructField('genre', StringType(), False),
        StructField('tweet_time', StringType(), False)
    ])

    df_music = sqlContext.createDataFrame(processed_music, schema)

    df_music = df_music.groupBy(['genre','tweet_time']).count()

    df_music.rdd.coalesce(1).map(lambda (genre,tweet_time,count): u"%s,%s,%s" % (tweet_time, genre, count)).saveAsTextFile(output_directory + "/summary")

    return

def main():
    '''
    read multiple processed parquet files
    :return:  text file of  aggregated data for visualisation
    '''

    appName = 'GroovyBear'
    conf = SparkConf().setAppName('GroovyBear')
    #conf.set("spark.app.id", 'Abhinav-Pranav-GroovyBear')
    path_raw_tweet = sys.argv[1]
    path_processed_tweet = sys.argv[2]
    output_directory = sys.argv[3]
    sc = SparkContext(conf=conf)
    assert sc.version >= '1.5.1'

    sqlContext = SQLContext(sc)

    genre_date_count(sc,sqlContext,path_processed_tweet,output_directory)

    counts(sc,sqlContext,path_raw_tweet,path_processed_tweet,output_directory)


if __name__ == '__main__': sys.exit(main())

