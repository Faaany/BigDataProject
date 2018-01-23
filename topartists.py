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

counts = songs.map(lambda song: song.Artist).map(lambda name: (name,1)).reduceByKey(lambda a,b:a+b).map(lambda x: (x[1],x[0]))

counts = counts.sortByKey(False)

# write to frontend
text_file = open("artists.txt", "w")
for count in counts.collect():
  text_file.write(str(count))
  text_file.write("\n")
text_file.close()
# write to HDFS folder
#sc.parallelize([filtered]).saveAsTextFile("songs")
#===========================================================================
