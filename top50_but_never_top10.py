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

top10songs = songs.filter(lambda song: song.Position <= 10).map(lambda song: (song.Name, song.Position)).reduceByKey(lambda a,b: min(a,b))
top50songs = songs.filter(lambda song: song.Position <= 50).map(lambda song: (song.Name, song.Position)).reduceByKey(lambda a,b: min(a,b))

top50_but_never_top10 = top50songs.subtractByKey(top10songs).map(lambda x: (x[1], x[0]))

# write to frontend
text_file = open("top50_but_never_top10.txt", "w")
for count in top50_but_never_top10.collect():
  text_file.write(str(count[0])+", "+count[1].encode("utf-8"))
  text_file.write("\n")
text_file.close()
# write to HDFS folder
#sc.parallelize([filtered]).saveAsTextFile("songs")
#===========================================================================
