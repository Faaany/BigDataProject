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
#create (name, streams) for each song
top10streams = songs.map(lambda song: (song.Name, song.Streams)).reduceByKey(lambda a,b: a+b).map(lambda x: (x[1], x[0]))
#sort keys
top10streams = top10streams.sortByKey(False)
#write all songs and the number of their streams
text_file = open("top10_streamed_songs.txt", "w")
for song in top10streams.take(10):
  text_file.write(str(song[0]) + ", " + song[1].encode("utf-8"))
  text_file.write("\n")
text_file.close()
#===========================================================================
