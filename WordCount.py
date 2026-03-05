from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, size, sum

logFile = "hamlet.txt"

# Create the Spark application entry point (driver-side session).
spark = SparkSession.builder.appName("WordCount").getOrCreate()
# Read the text file as a DataFrame with one row per line; cache for reuse.
logData = spark.read.text(logFile).cache()

# Split each line into words, count words per line, and sum all counts
# `select` + `size(split(...))` is a transformation (lazy, not executed yet).
wordsPerLine = logData.select(size(split(logData.value, ' ')).alias('word_count'))
# `collect()` is an action: triggers execution and brings the single result to the driver.
totalWords = wordsPerLine.agg(sum('word_count')).collect()[0][0]

print("Total number of words: %i" % totalWords)

# Release Spark resources cleanly when the job is done.
spark.stop()
