from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, count, monotonically_increasing_id, least, greatest, array_distinct

logFile = "hamlet.txt"

spark = SparkSession.builder.appName("WordPairs").getOrCreate()
logData = spark.read.text(logFile).cache()

# Assign unique IDs to lines and extract distinct words from each row.
words = logData.withColumn("line_id", monotonically_increasing_id()).select(
    col("line_id"),
    explode(split(col("value"), " ")).alias("word")
).distinct().filter(col("word") != "")

# Generate unique co-occurring word pairs per line using a self-join with consistent ordering (least/greatest).
wordPairs = words.alias("w1").join(
    words.alias("w2"),
    col("w1.line_id") == col("w2.line_id")
).select(
    least(col("w1.word"), col("w2.word")).alias("word1"),
    greatest(col("w1.word"), col("w2.word")).alias("word2")
)

# Top 20 most frequent pairs sorted in descending order 
top20 = wordPairs.groupBy("word1", "word2").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(20).collect()

print("Top 20 most frequent pairs appears on the same line:")
for row in top20:
    print("(%s, %s) %d" % (row["word1"], row["word2"], row["count"]))

spark.stop()
