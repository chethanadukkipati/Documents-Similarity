from pyspark import SparkContext,SparkConf

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
textrdd=sc.wholeTextFiles("/cosc6339_hw2/gutenberg-22/")
lowerwords=textrdd.map(lambda(file,contents):(file,contents.lower()))
removespecial=lowerwords.map(lambda(file,contents):(file[57:],re.sub('[^a-zA-Z0-9]',' ',contents)))
splitingwords=removespecial.map(lambda(file,contents):(file,contents.split()))
numberof_words=splitingwords.flatMap(lambda(file,contents):[(file,len(contents),word) for word in contents])
count_words=numberof_words.map(lambda w:(w,1))
no_words=count_words.reduceByKey(add,numPartitions=15)
questiontext=sc.textFile("/bigd11/outputass21small")
#no1_words=questiontext.flatMap(lambda x:x.split(",")[0])
list1=[]
for value in questiontext.take(questiontext.count()):
	list1.append(value.split(",")[0][3:-1])	
#print(list1)
no1_words=no_words.filter(lambda (x,y):x[2] in list1)
no2_words=no1_words.map(lambda (x,y):(x[2],(x[0],truediv(y,x[1]))))
no3_words=no2_words.map(lambda (x,y):(x,[y])).reduceByKey(add,numPartitions=15)
#no4_words=no3_words.map(lambda (x,y):y)
no3_words.saveAsTextFile("/bigd11/testingout2")

