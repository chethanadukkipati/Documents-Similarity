# -*- coding: utf-8 -*-
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.types import Row


from pyspark.streaming import StreamingContext
from operator import add,truediv,mul
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
import re
 
conf=SparkConf()
conf.setAppName("Inverted Index")
conf.set("spark.executor.memory","4g")

stop_words = set(stopwords.words('english'))

sc=SparkContext(conf=conf)
sqlContext=sql.SQLContext(sc)

textrdd=sc.wholeTextFiles("/cosc6339_hw2/gutenberg-500/")
lowerwords=textrdd.map(lambda(file,contents):(file,contents.lower()))
removespecial=lowerwords.map(lambda(file,contents):(file[57:],re.sub('[^a-zA-Z0-9]',' ',contents)))
splitingwords=removespecial.map(lambda(file,contents):(file,contents.split()))
numberof_words=splitingwords.flatMap(lambda(file,contents):[(file,len(contents),word) for word in contents])
count_words=numberof_words.map(lambda w:(w,1))
no_words=count_words.reduceByKey(add,numPartitions=5)
questiontext=sc.textFile("/bigd11/output_t1_m_5")

list1=[]
for value in questiontext.take(questiontext.count()):
	list1.append(value.split(",")[0][3:-1])
no1_words=no_words.filter(lambda (x,y):x[2] in list1)
no2_words=no1_words.map(lambda (x,y):(x[2],(x[0],truediv(y,x[1]))))
no3_words=no2_words.map(lambda (x,y):(x,[y])).reduceByKey(add,numPartitions=5)

dataframe_result=no3_words.toDF(["Word","List"])
dataframe_result.write.format("com.databricks.spark.avro").save("/bigd11/output3b53.avro")


