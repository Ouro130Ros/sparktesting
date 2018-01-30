import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

NAMENODE_URI = 'hdfs://ec2-35-168-8-255.compute-1.amazonaws.com/'

sc = SparkSession.builder.appName("Playing With Spark").getOrCreate()

hdfsQueryFile = sc.textFile(NAMENODE_URI + QUERY)
print hdfsQueryFile.first()