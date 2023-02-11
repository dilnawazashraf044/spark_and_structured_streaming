import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext()
print("Testing map function")
rdd=sc.parallelize([3, 4, 5]).map(lambda x: range(1, x)).collect()
print(rdd)
print("Testing flat map function")
rdd=sc.parallelize([3,4,5]).flatMap(lambda x:[x,x*x]).collect()
print(rdd)
print("Testing the textfile..")
lines=sc.textFile("greetings.txt")
print(lines.map(lambda line: line.split()).collect())
print(lines.flatMap(lambda line: line.split()).collect())
