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
#filter artists and create (name,1) for each artist
artists = songs.map(lambda song: song.Artist).map(lambda name: (name,1))
#reduce artists
artists = artists.reduceByKey(lambda a,b:a+b).map(lambda x: (x[1],x[0]))
#sort keys
artists = artists.sortByKey(False)
#write all artists
text_file = open("topartists.txt", "w")
for song in counts.collect():
  text_file.write(str(song[0]) + ", " + song[1].encode("utf8"))
  text_file.write("\n")
text_file.close()
#===========================================================================
