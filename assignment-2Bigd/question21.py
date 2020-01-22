from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from operator import add
from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
import re 
stop_words = set(stopwords.words('english'))

conf=SparkConf()
conf.setAppName("WordFrequency")
conf.set("spark.executor.memory","2g")

sc=SparkContext(conf=conf)
text=sc.textFile("/cosc6339_hw2/gutenberg-500/")
#text=re.sub(r'<.*,?>}{[]!%','',text)
#text = re.sub(r'[^a-zA-Z0-9 ]','',text)
def func(lines):
	lines=lines.lower();
        lines=re.sub(r'[^a-zA-Z0-9]',' ',lines)
	lines=lines.split();
	return lines
word1 = text.flatMap(func)
#words=word1.map(lambda s: s.replace("string",""))
#stopwords=sc.textFile("file:///home2/cosc6339/bigd11/nltk_data/corpora/stopwords/")
stop_words=set(stopwords.words())
#stop_words1=stop_words.flatMap(lambda line:line.split());
#words=[word for word in word1 if word not in stop_words]
words=word1.filter(lambda x:x not in stop_words)
wcounts = words.map(lambda w: (w, 1) )
counts = wcounts.reduceByKey(add, numPartitions=15)
#output = counts.map(lambda (k,v): (v,k)).sortByKey(False).take(20)
output=counts.sortBy(lambda a:a[1],ascending=False,numPartitions=15)
sc.parallelize(output.take(1000)).saveAsTextFile("/bigd11/output_t1_m_15")


