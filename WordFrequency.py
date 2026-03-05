from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, count

logFile = "hamlet.txt"

# Initialize Spark driver session for this job.
spark = SparkSession.builder.appName("WordFrequency").getOrCreate()
# Load file as one line per row and cache it since it is reused downstream.
logData = spark.read.text(logFile).cache()

# Split and explode text into words, then group by count.
# `explode` turns each line's word array into many rows (one row per word).
wordCounts = logData.select(explode(split(col('value'), ' ')).alias('word')).filter(col('word') != '').groupBy('word').agg(count('*').alias('count'))

# Top 20 most frequent words sorted in descending order
# Sort by frequency (highest first) and keep only the first 20 rows.
top20 = wordCounts.orderBy(col('count').desc()).limit(20)

print("Top 20 most frequent words:")
# `show` is an action: it triggers Spark execution and prints the result table.
top20.show(20, truncate=False)

# Stop Spark to free cluster/local resources.
spark.stop()
