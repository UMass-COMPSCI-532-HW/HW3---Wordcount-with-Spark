from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, size, sum

logFile = "hamlet.txt"

spark = SparkSession.builder.appName("WordCount").getOrCreate()
logData = spark.read.text(logFile).cache()

# Split each line into words, count words per line, and sum all counts
wordsPerLine = logData.select(size(split(logData.value, ' ')).alias('word_count'))
totalWords = wordsPerLine.agg(sum('word_count')).collect()[0][0]

print("Total number of words: %i" % totalWords)

spark.stop()
