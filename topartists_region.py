from pyspark import SparkConf, SparkContext
import re
from song import Song
#==========================================================================
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

#group data by region
regions = songs.groupBy(lambda song: song.Region).collect()

text_file = open("topartists_region.txt","w")
for region in regions:
    text_file.write(str(region[0])+": ")
    ranking = sc.parallelize(region[1]).map(lambda song: (song.Artist,1)).reduceByKey(lambda a,b: a+b).map(lambda x: (x[1],x[0])).sortByKey(False)
    text_file.write(ranking.first()[1].encode("utf8"))
    text_file.write("\n")

text_file.close()

#sc.parallelize([filtered]).saveAsTextFile("songs")
#===========================================================================
