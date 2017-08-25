from pyspark import SparkConf, SparkContext

# Spin up SparkContext using < 2.0 API.
conf = SparkConf()
sc = SparkContext(conf = conf)

# Run word count on text from Pride and Prejudice.
text_file = sc.textFile("hdfs://tmp/ooziewrapper_example/pride_and_prejudice.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://tmp/ooziewrapper_example/pnp_wordcount.txt")
