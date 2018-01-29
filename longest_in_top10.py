#===========================================================================
from pyspark import SparkConf, SparkContext
import re
from song import *
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
#filter songs in the top 10 and create (name,1) for each song
songcounts = songs.filter(lambda song: song.Position <= 10).map(lambda song: (song.Name, 1))
#reduce songcounts
songcount = songcounts.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0]))
#sort keys
songcount = songcount.sortByKey(False)
#write all top 10 songs and the number of days the played
text_file = open("longest_in_top10.txt", "w")
for song in songcount.collect():
  text_file.write(str(song[0]) + ", " + song[1].encode("utf8"))
  text_file.write("\n")
text_file.close()
#===========================================================================
