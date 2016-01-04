# -*- coding: utf-8 -*-
import sys,time
import requests,re
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def itunes_cache(directory=None):
    '''
    Enables caching, in the specified cache directory
    @param  __cache_dir:  string
    '''
    import itunes
    
    return itunes.enable_caching(directory) if directory else False

def music_info(artist='', song=''):
    '''
    Get genre information from given artist and song
    
    Returns cleaned artist and song names along with genre
    '''
    import itunes
    
    term_artist = artist.strip() if artist else ''
    term_song   = song.strip() if song else ''
    
    payload = {
        'query'     : u'{0} {1}'.format(term_artist, term_song).lower(),
        'media'     : 'music',
        'store'     : 'GB'
        }

    if not payload['query']:
        return None
    
    try:    
        response = itunes.search(**payload)
    
        if response:
            # Get the genre for the given artist and song, along with corrected artist and song names
            #
            # If the song name is not given, it'll fetch the most popular song by the given artist
            artist_name, song_name, genre_name = response[0].get_artist().get_name(),\
                                                 response[0].get_name(),\
                                                 response[0].get_genre()
        
        # But if the song name is given and is incorrect or doesn't fetch a result, just use the artist info and get the stuff
        elif term_artist and term_artist.strip():
            # Get the primary genre for the given artist if nothing matches with the song
        
            payload.update({'query' : term_artist.lower()})
        
            response = itunes.search(**payload)
        
            if response:
                artist_name, song_name, genre_name  = response[0].get_name(),\
                                                    response[0].get_tracks(limit=1).get_name(),\
                                                    response[0].get_genre()
            else:
                return None
        
        else:
            return None
            
        return artist_name, song_name, genre_name
    except Exception, e:
        return None

def top_10_songs(genre=None):
    '''
    Find top 10 songs in the given genre
    
    @param  genre   string
    '''
    if not genre:
        return None
    
    '''
    Genre names and IDs processed from Apple Enterprise Partner Feed. These do not change.
    '''
    genres = { 'music' : 34, \
                'blues' : 2, \
                'chicago blues' : 1007, \
                'classic blues' : 1009, \
                'contemporary blues' : 1010, \
                'country blues' : 1011, \
                'delta blues' : 1012, \
                'electric blues' : 1013, \
                'acoustic blues' : 1210, \
                'comedy' : 3, \
                'novelty' : 1167, \
                'standup comedy' : 1171, \
                'children\'s music' : 4, \
                'lullabies' : 1014, \
                'sing-along' : 1015, \
                'stories' : 1016, \
                'classical' : 5, \
                'avant-garde' : 1017, \
                'baroque' : 1018, \
                'chamber music' : 1019, \
                'chant' : 1020, \
                'choral' : 1021, \
                'classical crossover' : 1022, \
                'early music' : 1023, \
                'impressionist' : 1024, \
                'medieval' : 1025, \
                'minimalism' : 1026, \
                'modern composition' : 1027, \
                'opera' : 1028, \
                'orchestral' : 1029, \
                'renaissance' : 1030, \
                'romantic' : 1031, \
                'wedding music' : 1032, \
                'high classical' : 1211, \
                'country' : 6, \
                'alternative country' : 1033, \
                'americana' : 1034, \
                'bluegrass' : 1035, \
                'contemporary bluegrass' : 1036, \
                'contemporary country' : 1037, \
                'country gospel' : 1038, \
                'honky tonk' : 1039, \
                'outlaw country' : 1040, \
                'traditional bluegrass' : 1041, \
                'traditional country' : 1042, \
                'urban cowboy' : 1043, \
                'electronic' : 7, \
                'ambient' : 1056, \
                'downtempo' : 1057, \
                'electronica' : 1058, \
                'idm/experimental' : 1060, \
                'industrial' : 1061, \
                'holiday' : 8, \
                'chanukah' : 1079, \
                'christmas' : 1080, \
                'christmas: children\'s' : 1081, \
                'christmas: classic' : 1082, \
                'christmas: classical' : 1083, \
                'christmas: jazz' : 1084, \
                'christmas: modern' : 1085, \
                'christmas: pop' : 1086, \
                'christmas: r&b' : 1087, \
                'christmas: religious' : 1088, \
                'christmas: rock' : 1089, \
                'easter' : 1090, \
                'halloween' : 1091, \
                'holiday: other' : 1092, \
                'thanksgiving' : 1093, \
                'opera' : 9, \
                'singer/songwriter' : 10, \
                'alternative folk' : 1062, \
                'contemporary folk' : 1063, \
                'contemporary singer/songwriter' : 1064, \
                'folk-rock' : 1065, \
                'new acoustic' : 1066, \
                'traditional folk' : 1067, \
                'jazz' : 11, \
                'big band' : 1052, \
                'avant-garde jazz' : 1106, \
                'contemporary jazz' : 1107, \
                'crossover jazz' : 1108, \
                'dixieland' : 1109, \
                'fusion' : 1110, \
                'latin jazz' : 1111, \
                'mainstream jazz' : 1112, \
                'ragtime' : 1113, \
                'smooth jazz' : 1114, \
                'hard bop' : 1207, \
                'trad jazz' : 1208, \
                'cool' : 1209, \
                'latino' : 12, \
                'latin jazz' : 1115, \
                'contemporary latin' : 1116, \
                'pop latino' : 1117, \
                'raíces' : 1118, \
                'reggaeton y hip-hop' : 1119, \
                'baladas y boleros' : 1120, \
                'alternativo & rock latino' : 1121, \
                'regional mexicano' : 1123, \
                'salsa y tropical' : 1124, \
                'new age' : 13, \
                'environmental' : 1125, \
                'healing' : 1126, \
                'meditation' : 1127, \
                'nature' : 1128, \
                'relaxation' : 1129, \
                'travel' : 1130, \
                'pop' : 14, \
                'adult contemporary' : 1131, \
                'britpop' : 1132, \
                'pop/rock' : 1133, \
                'soft rock' : 1134, \
                'teen pop' : 1135, \
                'r&b/soul' : 15, \
                'contemporary r&b' : 1136, \
                'disco' : 1137, \
                'doo wop' : 1138, \
                'funk' : 1139, \
                'motown' : 1140, \
                'neo-soul' : 1141, \
                'quiet storm' : 1142, \
                'soul' : 1143, \
                'soundtrack' : 16, \
                'foreign cinema' : 1165, \
                'musicals' : 1166, \
                'original score' : 1168, \
                'soundtrack' : 1169, \
                'tv soundtrack' : 1172, \
                'dance' : 17, \
                'breakbeat' : 1044, \
                'exercise' : 1045, \
                'garage' : 1046, \
                'hardcore' : 1047, \
                'house' : 1048, \
                'jungle/drum\'n\'bass' : 1049, \
                'techno' : 1050, \
                'trance' : 1051, \
                'hip-hop/rap' : 18, \
                'alternative rap' : 1068, \
                'dirty south' : 1069, \
                'east coast rap' : 1070, \
                'gangsta rap' : 1071, \
                'hardcore rap' : 1072, \
                'hip-hop' : 1073, \
                'latin rap' : 1074, \
                'old school rap' : 1075, \
                'rap' : 1076, \
                'underground rap' : 1077, \
                'west coast rap' : 1078, \
                'world' : 19, \
                'afro-beat' : 1177, \
                'afro-pop' : 1178, \
                'cajun' : 1179, \
                'celtic' : 1180, \
                'celtic folk' : 1181, \
                'contemporary celtic' : 1182, \
                'drinking songs' : 1184, \
                'indian pop' : 1185, \
                'japanese pop' : 1186, \
                'klezmer' : 1187, \
                'polka' : 1188, \
                'traditional celtic' : 1189, \
                'worldbeat' : 1190, \
                'zydeco' : 1191, \
                'caribbean' : 1195, \
                'south america' : 1196, \
                'middle east' : 1197, \
                'north america' : 1198, \
                'hawaii' : 1199, \
                'australia' : 1200, \
                'japan' : 1201, \
                'france' : 1202, \
                'africa' : 1203, \
                'asia' : 1204, \
                'europe' : 1205, \
                'south africa' : 1206, \
                'alternative' : 20, \
                'college rock' : 1001, \
                'goth rock' : 1002, \
                'grunge' : 1003, \
                'indie rock' : 1004, \
                'new wave' : 1005, \
                'punk' : 1006, \
                'rock' : 21, \
                'adult alternative' : 1144, \
                'american trad rock' : 1145, \
                'arena rock' : 1146, \
                'blues-rock' : 1147, \
                'british invasion' : 1148, \
                'death metal/black metal' : 1149, \
                'glam rock' : 1150, \
                'hair metal' : 1151, \
                'hard rock' : 1152, \
                'metal' : 1153, \
                'jam bands' : 1154, \
                'prog-rock/art rock' : 1155, \
                'psychedelic' : 1156, \
                'rock & roll' : 1157, \
                'rockabilly' : 1158, \
                'roots rock' : 1159, \
                'singer/songwriter' : 1160, \
                'southern rock' : 1161, \
                'surf' : 1162, \
                'tex-mex' : 1163, \
                'christian & gospel' : 22, \
                'ccm' : 1094, \
                'christian metal' : 1095, \
                'christian pop' : 1096, \
                'christian rap' : 1097, \
                'christian rock' : 1098, \
                'classic christian' : 1099, \
                'contemporary gospel' : 1100, \
                'gospel' : 1101, \
                'praise & worship' : 1103, \
                'southern gospel' : 1104, \
                'traditional gospel' : 1105, \
                'vocal' : 23, \
                'standards' : 1173, \
                'traditional pop' : 1174, \
                'vocal jazz' : 1175, \
                'vocal pop' : 1176, \
                'reggae' : 24, \
                'dancehall' : 1183, \
                'roots reggae' : 1192, \
                'dub' : 1193, \
                'ska' : 1194, \
                'easy listening' : 25, \
                'bop' : 1053, \
                'lounge' : 1054, \
                'swing' : 1055, \
                'j-pop' : 27, \
                'enka' : 28, \
                'anime' : 29, \
                'kayokyoku' : 30, \
                'fitness & workout' : 50, \
                'k-pop' : 51, \
                'karaoke' : 52, \
                'instrumental' : 53, \
                'brazilian' : 1122, \
                'axé' : 1220, \
                'bossa nova' : 1221, \
                'choro' : 1222, \
                'forró' : 1223, \
                'frevo' : 1224, \
                'mpb' : 1225, \
                'pagode' : 1226, \
                'samba' : 1227, \
                'sertanejo' : 1228, \
                'baile funk' : 1229, \
                'spoken word' : 50000061, \
                'disney' : 50000063, \
                'french pop' : 50000064, \
                'german pop' : 50000066, \
                'german folk' : 50000068 \
            }
    genre_id        =  genres[genre.strip().lower()]
    top_songs_url   = 'https://itunes.apple.com/us/rss/topsongs/genre={0}/json'.format(genre_id)
    response        = requests.post(top_songs_url, timeout = 1)
    
    if response.status_code != 200:
        print u'Error: Invalid Response: {0}'.format(response.status_code)
        return None
    
    song_artist_info = response.json()['feed']['entry']
    
    # return list of ten [song, artist]
    return [info['title']['label'].split(' - ') for info in song_artist_info]

def get_data(sc, sqlContext, input_path, output_path):
    '''
    Prepares tweets with processed song, artist and genre names for saving
    
    :param sc: spark context
    :param sqlContext:
    :return: dataframe stored with matching genre , proper song and artist name
    '''
    
    # df format -coloumn names -  raw_tweet|processed_tweet|song|artist|tweet_time

    try:

        tfile       = sqlContext.read.parquet(input_path)

        processed_music = sqlContext.read.parquet(output_path)

        # Get only that data which was not processed earlier

        r = processed_music.agg({"tweet_time": "max"}).withColumnRenamed('max(tweet_time)','t')

        condition = [tfile.tweet_time > r.t]

        unmatched_data = tfile.join(r,condition).drop(r.t)


        df_tweet    = unmatched_data.filter(tfile.song != '').filter(tfile.artist != '')

        # getting the ture song artist and genre by comparing tweet data to with itunes api
        music_match = df_tweet.map(lambda row: (row.raw_tweet,row.processed_tweet,music_info(artist=row.artist, song=row.song),row.tweet_time)). \
                            filter(lambda (a,b,(c),d): c is not None)

        music_match = music_match.map(lambda (raw_tweet,processed_tweet,(artist,song,genre),tweet_time): (raw_tweet,processed_tweet,artist,song,genre,tweet_time))

                # storing the matched data from the itunes api in dataframe format
        schema = StructType([ \
                    StructField('raw_tweet', StringType(), False), \
                    StructField('processed_tweet', StringType(), False), \
                    StructField('artist', StringType(), False), \
                    StructField('song', StringType(), False), \
                    StructField('genre', StringType(), False), \
                    StructField('tweet_time', IntegerType(), False) \
                ])
        print music_match.take(10)
        df_music_match = sqlContext.createDataFrame(music_match, schema)

        df_music_match.write.mode('append').parquet(output_path)

    except Exception:
        # When the output processed itunes folder do not exit(First Run)

        tfile       = sqlContext.read.parquet(input_path)
        df_tweet    = tfile.filter(tfile.song != '').filter(tfile.artist != '')

        # getting the ture song artist and genre by comparing tweet data to with itunes api
        music_match = df_tweet.map(lambda row: (row.raw_tweet,row.processed_tweet,music_info(artist=row.artist, song=row.song),row.tweet_time)). \
                            filter(lambda (a,b,(c),d): c is not None)

        music_match = music_match.map(lambda (raw_tweet,processed_tweet,(artist,song,genre),tweet_time): (raw_tweet,processed_tweet,artist,song,genre,tweet_time))

        # storing the matched data from the itunes api in dataframe format
        schema = StructType([ \
                    StructField('raw_tweet', StringType(), False), \
                    StructField('processed_tweet', StringType(), False), \
                    StructField('artist', StringType(), False), \
                    StructField('song', StringType(), False), \
                    StructField('genre', StringType(), False), \
                    StructField('tweet_time', IntegerType(), False) \
                ])

        df_music_match = sqlContext.createDataFrame(music_match, schema)

        df_music_match.write.mode('append').parquet(output_path)

    return


# Enable cache in a directory named 'cache'
itunes_cache('cache')

def main():
    '''
    Takes 2 input arguments - path to the input file of processed twitter data
    path to output file to save the data frame in parquet format by matched music from iTunes API
    :return:
    '''
    # Enables cache in a directory named 'cache'
    itunes_cache('cache')

    appName = 'GroovyBear'
    conf = SparkConf().setAppName('GroovyBear')
    #conf.set("spark.app.id", 'Abhinav-Pranav-GroovyBear')

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    sc = SparkContext(conf=conf)
    assert sc.version >= '1.5.1'

    sqlContext = SQLContext(sc)

    get_data(sc, sqlContext, input_path, output_path)
    
    # Test Cases:
    #
    print 'Test Cases: '
    #
    # 1. Both artist and song are correct, should return correct names and stuff
    print '1 ', music_info(artist='Jay Sean', song='Holding On')

    # 2. Artist is correct but song is incorrect, should return correct artist name, most popular song and correct genre
    print '2 ', music_info(artist='Jay Sean', song='He did not sing this song')

    # 3. Artist is given, should return correct artist name, most popular song and correct genre
    print '3 ', music_info(artist='Jay Sean')

    # 4. Nothing is correct, should return None
    print '4 ', music_info(artist='Bloopie Boxie Boom')

    # 5. Nothing is correct, should return None
    print '5 ', music_info(song='Bloopie Boxie Boom')

    # 6. Nothing given at all, should return None
    print '6 ', music_info()

    # 7. Get top 10 songs in hip-hop/rap - case insensitive, trims leading/trailing whitespace
    print '7 ', top_10_songs('  HiP-hop/RaP  ')

if __name__ == '__main__': sys.exit(main())
