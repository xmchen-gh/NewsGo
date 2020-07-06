import sys
from pyspark.sql import SparkSession

import csv
import nltk
import geograpy
import pandas
from collections import Counter
import matplotlib.pyplot as plt

from mordecai import Geoparser
from geopy.geocoders import Nominatim



if __name__ == "__main__":

    geo = Geoparser()
    geolocator = Nominatim(user_agent="temp")
    
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    for hour in range(0,23):
        
        for five_min in range(0,12):

            for minute in range(5):
                #lines = spark.read.csv(sys.argv[1], header = 'True')
                #argv = cat-twitter-dump/23/
                if hour<10:
                    strHour = '0'+str(hour)
                else:
                    strHour = str(hour)
                    
                current_time = 5*(five_min) + minute
                if current_time < 10:
                    file_name = sys.argv[1] + strHour + '/' + '0'+str(current_time)+'.json.bz2'
                else:
                    file_name = sys.argv[1] + strHour +'/' + str(current_time)+'.json.bz2'
                lines = spark.read.json(file_name)

                time = []
                loc = []
                coord_lat = []
                coord_lon = []
                text = []
                url = []

                for j, row in enumerate(lines.collect()):
                    if row['text'] != None: # from text
                        place = geo.geoparse(row['text'])
                        if place != []:
                            for p in range(len(place)):
                                if 'geo' in place[p].keys():
                                    time.append(row['created_at'])
                                    try:
                                        loc.append(place[p]['geo']['place_name']+', '+place[p]['geo']['admin1'])
                                    except TypeError:
                                        loc.append('TypeError exception')
                                    coord_lat.append(str(place[p]['geo']['lat']))
                                    coord_lon.append(str(place[p]['geo']['lon']))
                                    text.append(row['text'])
                                    try:
                                        url.append(row['entities']['media'][0]['media_url'])
                                    except TypeError:
                                        url.append(None)                    

                    if row['place'] != None: #from geo
                        try:
                            location = geolocator.geocode(row['place']['full_name'])
                        except:
                            continue
                        if location != None:
                            coord0 = [location.latitude, location.longitude]
                        if row['geo'] != None:
                            coord0 = row['geo']['coordinates']
                        if location != None or row['geo'] != None:
                            time.append(row['created_at'])            
                            loc.append(row['place']['full_name'])
                            coord_lat.append(str(coord0[0]))
                            coord_lon.append(str(coord0[1]))
                            text.append(row['text'])
                            try:
                                url.append(row['entities']['media'][0]['media_url'])
                            except TypeError:
                                url.append(None)
                data = {'time':time, 'loc':loc, 'coord_lat':coord_lat, 'coord_lon':coord_lon, 'text':text, 'url':url}
                df = pandas.DataFrame(data, columns = ['time','loc','coord_lat', 'coord_lon','text','url'])


                #df_spark = spark.createDataFrame(df)
                URL = 'jdbc:postgresql://ec2-**-***-***-***.us-west-1.compute.amazonaws.com:5432/***'
                spark.createDataFrame(df).write.format("jdbc").option("url", URL).option("dbtable", "public.timeNPL_"+ strHour +"_"+str(current_time)).option("user", "***").option("password", "********").mode("append").save()
