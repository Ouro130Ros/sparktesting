import pyspark
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

MASTER_URI = 'ec2-35-168-8-255.compute-1.amazonaws.com'
NAMENODE_URI = 'hdfs://' + MASTER_URI + '/'
SPARK_URI = 'spark://' + MASTER_URI + ":7077"

session = SparkSession.builder.appName("SparkTests").getOrCreate()
conf = SparkConf().setAppName("SparkTests")
sc = SparkContext(conf = conf)

hdfsQueryFile = sc.textFile(NAMENODE_URI + 'QUERY')
hdfsQuerySplit = hdfsQueryFile.map(lambda l: l.split("|"))

queryTuples = hdfsQuerySplit.map(lambda l: 
	(
		str(datetime.datetime.now()).split()[0],
		1,
		long(l[0]),
		l[1],
		l[2],
		l[3],
		l[4],
		l[5],
		int(l[6]),
		l[7],
		long(l[8]),
		l[9],
		l[10],
		l[11],
		long(l[12]),
		l[13],
		l[14]
	)
)

queryFields = [
	StructField('TPR_AS_OF_DT', DateType(),True)
	,StructField('BATCH_RUN_ID', IntegerType(), True)
	,StructField('TDR_TIME', LongType(),True)
	,StructField('HOST_NAME',StringType(),True)
	,StructField('CLIENT_GROUP',StringType(),True)
	,StructField('CLIENT',StringType(),True)
	,StructField('REQUEST_TYPE',StringType(),True)
	,StructField('COUNTRY_CODE',IntegerType(),True)
	,StructField('RESPONSE',StringType(),True)
	,StructField('SPID',LongType(),True)
	,StructField('DATA_LOCATION',StringType(),True)
	,StructField('DATA_SOURCE',StringType(),True)
	,StructField('ORIGIN',StringType(),True)
	,StructField('NSN',LongType(),True)
	,StructField('NUM',StringType(),True)
	,StructField('COUNTRY',StringType(),True)
]
querySchema = StructType(queryFields)

dfQuery = session.createDataFrame(queryTuples, querySchema)
dfQuery.printSchema()
dfQuery.write.parquet(NAMENODE_URI + 'QUERY.parquet')