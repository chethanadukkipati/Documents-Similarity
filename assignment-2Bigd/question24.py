from pyspark import SparkContext,SparkConf

from pyspark.streaming import StreamingContext
from operator import add,truediv,mul
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize

import re

conf=SparkConf()
conf.setAppName("Duplicates docs")
conf.set("spark.executor.memory","4g")

stop_words = set(stopwords.words('english'))

sc=SparkContext(conf=conf)
textrdd=sc.wholeTextFiles("/cosc6339_hw2/gutenberg-500")
lowerwords=textrdd.map(lambda(file,contents):(file,contents.lower()))
removespecial=lowerwords.map(lambda(file,contents):(file[57:],re.sub('[^a-zA-Z0-9]',' ',contents)))
splitingwords=removespecial.map(lambda(file,contents):(file,contents.split()))
numberof_words=splitingwords.flatMap(lambda(file,contents):[(file,len(contents),word) for word in contents])
count_words=numberof_words.map(lambda w:(w,1))
no_words=count_words.reduceByKey(add)
questiontext=sc.textFile("/bigd11/output_t1_m_5")
#no1_words=questiontext.flatMap(lambda x:x.split(",")[0])
list1=[]
for value in questiontext.take(questiontext.count()):
        list1.append(value.split(",")[0][3:-1])
#print(list1)
no1_words=no_words.filter(lambda (x,y):x[2] in list1)
no2_words=no1_words.map(lambda (x,y):(x[2],(x[0],truediv(y,x[1]))))
no3_words=no2_words.map(lambda (x,y):(x,[y])).reduceByKey(add,numPartitions=5)
no4_words=no3_words.map(lambda (x,y):y)
list2=[]
def func(lines):
        for i in range(0,len(lines)):
                for j in range(i+1,len(lines)):
                        if len(lines)==1:
                                break
                        weight_1=lines[i][1]
                        weight_2=lines[j][1]
                        similarity_value=((lines[i][0],lines[j][0]),mul(weight_1,weight_2))
                        list2.append(similarity_value)
        return list2
textdata=no4_words.flatMap(func)
textdata1=textdata.reduceByKey(add)
output=textdata1.sortBy(lambda a:a[1],ascending=False,numPartitions=5)
sc.parallelize(output.take(10)).saveAsTextFile("/bigd11/output_t3_m_5")


