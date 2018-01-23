from pyspark import SparkConf, SparkContext
import re
from song import *

#run on cluster
confCluster = SparkConf()
confCluster.setAppName("Spark Test Cluster")

#run on local machine
confLocal = SparkConf().setMaster("local").setAppName("Spark Test Local")

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
#write top 10 of the longest played songs to frontend
text_file = open("longest_in_top10_songs.txt", "w")
for song in songcount.take(10):
  text_file.write(str(song))
  text_file.write("\n")
text_file.close()
# write to HDFS folder
#sc.parallelize([filtered]).saveAsTextFile("songs")
#===========================================================================
