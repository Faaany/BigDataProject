##===========================================================================
from pyspark import SparkConf, SparkContext
import re
from song import Song
#===========================================================================
#setup spark
#(cluster)
confCluster = SparkConf()
confCluster.setAppName("Spark Test Cluster")
#(local machine)
confLocal = SparkConf().setMaster("local").setAppName("Spark Test Local")
#add custom song class
sc = SparkContext(conf = confCluster, pyFiles=["song.py"])
#===========================================================================
#load data
data = sc.textFile("data.txt")
#interpret data
songs = data.map(lambda line: Song(line))
#filter the top 10 songs
top10songs = songs.filter(lambda song: song.Position <= 10)
#get songs which are in the top10 of multiple countries and get the first appearance
first_appearance_in_top10 = top10songs.map(lambda song: ((song.Name, song.Region), song.Date)).distinct().reduceByKey(lambda x,y: min(x,y))
#extract dates when the songs was first in the top 10
first_appearance_in_top10 = first_appearance_in_top10.map(lambda x: (x[0][0], x[1])).distinct().map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y)
#filter out songs with only one date
first_appearance_in_top10 = first_appearance_in_top10.filter(lambda x: len(x[1]) > 1)
#define difference function
def differences(listing):
  first = min(listing)
  #calculate differences
  listing = list(map(lambda x: (x-first).days, listing))
  #order listing
  listing.sort()
  #remove first element
  listing.pop(0)
  #done
  return listing
#calculate time differences for each song
top10_spreading = first_appearance_in_top10.map(lambda x: (x[0], differences(x[1])))
#write all songs and their time differences
text_file = open("top10_spreading.txt", "w")
for song in top10spreading.collect():
  text_file.write(song[0].encode("utf-8") + ": ")
  for date in song[1]:
    text_file.write(str(date)+", ")
  text_file.write("\n")
text_file.close()
#===========================================================================
