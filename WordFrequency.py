from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, count

logFile = "hamlet.txt"

spark = SparkSession.builder.appName("WordFrequency").getOrCreate()
logData = spark.read.text(logFile).cache()

# Split and explode text into words, then group by count.
wordCounts = logData.select(explode(split(col('value'), ' ')).alias('word')).filter(col('word') != '').groupBy('word').agg(count('*').alias('count'))

# Top 20 most frequent words sorted in descending order
top20 = wordCounts.orderBy(col('count').desc()).limit(20)

print("Top 20 most frequent words:")
top20.show(20, truncate=False)

spark.stop()
