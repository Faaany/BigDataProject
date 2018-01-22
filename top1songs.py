from pyspark import SparkConf, SparkContext
import re
from song import Song

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

filtered = songs.filter(lambda x: x.Position == "1").map(lambda song: song.Name).distinct()
# write to frontend
text_file = open("songs.txt", "w")
for song in filtered.collect():
  text_file.write(song.encode("utf8"))
  text_file.write("\n")
text_file.close()
# write to HDFS folder
#sc.parallelize([filtered]).saveAsTextFile("songs")
#===========================================================================
