#===========================================================================
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
#filter top 10 songs and create (region, name) for each song
top10songs = songs.filter(lambda song: song.Position <= 10).map(lambda song: (song.Region, song.Name))
#filter distinct songs
top10songs = top10songs.distinct()
#reduce songs per country (create a list)
top10songs = top10songs.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y)
#write all top 10 songs per country
text_file = open("top10_songs_per_region.txt", "w")
for region in top10songs.collect():
  text_file.write(str(region[0]) + ": ")
  for song in region[1]:
    text_file.write(song.encode("utf8") + ", ")
  text_file.write("\n")
text_file.close()
#write number of top 10 songs per country
text_file = open("most_changing_regions.txt", "w")
for region in top10songs.collect():
  text_file.write(str(region[0]) + ", " + str(len(region[1])))
  text_file.write("\n")
text_file.close()
#===========================================================================
