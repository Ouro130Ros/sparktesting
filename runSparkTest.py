import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

MASTER_URI = 'ec2-35-168-8-255.compute-1.amazonaws.com'
NAMENODE_URI = 'hdfs://' + MASTER_URI + '/'
SPARK_URI = 'spark://' + MASTER_URI + ":7077"

conf = SparkConf().setAppName("SparkTests").setMaster(SPARK_URI)
sc = SparkContext(conf = conf)

hdfsQueryFile = sc.textFile(NAMENODE_URI + QUERY)
print hdfsQueryFile.first()