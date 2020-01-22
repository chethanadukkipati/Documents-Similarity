from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.types import Row
from pyspark.streaming import StreamingContext
from operator import add,truediv,mul
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
from itertools import combinations
import re
#import org.apache.spark.sql.SparkSession
#import com.databricks.spark.avro 
conf=SparkConf()
conf.setAppName("Similarity Index Avro")
conf.set("spark.executor.memory","4g")

stop_words = set(stopwords.words('english'))

sc=SparkContext(conf=conf)
sqlContext=sql.SQLContext(sc)

dataframe_read=sqlContext.read.format("com.databricks.spark.avro").load("/bigd11/output3b51.avro")
rdd_1 = dataframe_read.rdd.map(tuple)
list2=[]
def func(lines):
	file_pairs, index = zip(*lines)
    	file_pairs = combinations(file_pairs, 2)
    	index = map(lambda x: x[0] * x[1], combinations(index, 2))
	return [(i,j) for i,j in zip(file_pairs, index)]

filenames_pair=rdd_1.flatMap(lambda x:func(x[1]))
similarity_index=filenames_pair.reduceByKey(add)
dataframe_result=similarity_index.toDF(["Word","List"])
dataframe_result.write.format("com.databricks.spark.avro").save("/bigd11/output3bb51.avro")

