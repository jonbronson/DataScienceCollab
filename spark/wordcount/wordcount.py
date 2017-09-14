from pyspark import SparkContext, SparkConf

sc = SparkContext("local", "Word Count")

text_file = sc.textFile("file.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("counts")
