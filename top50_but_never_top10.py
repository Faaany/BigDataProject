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
#filter top 10 and top 50 for each song and create (name, position) for each song
top10songs = songs.filter(lambda song: song.Position <= 10).map(lambda song: (song.Name, song.Position)).reduceByKey(lambda a,b: min(a,b))
top50songs = songs.filter(lambda song: song.Position <= 50).map(lambda song: (song.Name, song.Position)).reduceByKey(lambda a,b: min(a,b))
#remove all songs from the top 50 which are in the top 10 as well
top50_but_never_top10 = top50songs.subtractByKey(top10songs).map(lambda x: (x[1], x[0]))
#write all top 50 songs which never reached the top 10
text_file = open("top50_but_never_top10.txt", "w")
for song in top50_but_never_top10.collect():
  text_file.write(str(song[0]) + ", " + song[1].encode("utf8"))
  text_file.write("\n")
text_file.close()
#===========================================================================
