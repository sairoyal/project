
m pyspark import SparkContext
sc = SparkContext()

# Creating an RDD of String with textFile() in python
lines = sc.textFile("/home/sairoyal/count")

# Calling the filter() Transformation
pythonlines = lines.filter(lambda line: "python" in line)

# Calling the first() action
print pythonlines.first()

# Persisting an RDD in memory
pythonlines.persist()
print pythonlines.count()



