from pyspark import SparkContext
sc = SparkContext()

# Creating an RDD by loading an external dataset
inputRDD = sc.textFile("/home/sairoyal/Downloads/apache-samples/error_log")

# Calling filter() Transformation in python
errorRDD = inputRDD.filter(lambda x: "error" in x)
#print errorRDD.collect()

# Union() Transformation in python
clientRDD = inputRDD.filter(lambda x: "client" in x)
badLinesRDD = inputRDD.union(clientRDD)
#print badLinesRDD.collect()

# Python error count using actions
#print badLinesRDD.count()
#for line in  badLinesRDD.take(5):
#	print line

# Passing function in python
word = errorRDD.filter(lambda s: "error" in s )
def containsError(s):
	return "error" in s
word = errorRDD.filter(containsError)	
print word.collect()
