import findspark
findspark.init()
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf=SparkConf().setMaster("local[2]").setAppName("TransformDemo2")
sc=SparkContext(conf=conf)
ssc=StreamingContext(sc,5)

rdd1=ssc.sparkContext.parallelize([1,2,3])
rdd2=ssc.sparkContext.parallelize([4,5,6])
rddQueue=[rdd1,rdd2]
numsDStream = ssc.queueStream(rddQueue)
plusOneDStream = numsDStream.map(lambda x : x+1)
plusOneDStream.pprint()

commonRdd=ssc.sparkContext.parallelize([7,8,9])
combinedDstream=numsDStream.transform(lambda rdd: rdd.union(commonRdd))
combinedDstream.pprint()
ssc.start()
ssc.stop(stopSparkContext=True, stopGraceFully=True)