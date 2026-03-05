from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, count, monotonically_increasing_id, least, greatest, array_distinct

logFile = "hamlet.txt"

# Start Spark session (driver entry point) for this job.
spark = SparkSession.builder.appName("WordPairs").getOrCreate()
# Read input as one row per line and cache because it feeds multiple transformations.
logData = spark.read.text(logFile).cache()

# Assign unique IDs to lines and extract distinct words from each row.
# `distinct()` here keeps one copy of each (line_id, word), avoiding duplicate same-word entries per line.
words = logData.withColumn("line_id", monotonically_increasing_id()).select(
    col("line_id"),
    explode(split(col("value"), " ")).alias("word")
).distinct().filter(col("word") != "")

# Generate unique co-occurring word pairs per line using a self-join with consistent ordering (least/greatest).
# Self-join on `line_id` creates all word combinations that appear in the same line.
wordPairs = words.alias("w1").join(
    words.alias("w2"),
    col("w1.line_id") == col("w2.line_id")
).select(
    # Canonical ordering prevents treating (a,b) and (b,a) as different pairs.
    least(col("w1.word"), col("w2.word")).alias("word1"),
    greatest(col("w1.word"), col("w2.word")).alias("word2")
)

# Top 20 most frequent pairs sorted in descending order 
# `collect()` is an action: executes the Spark plan and returns results to the driver as Python rows.
top20 = wordPairs.groupBy("word1", "word2").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(20).collect()

print("Top 20 most frequent pairs appears on the same line:")
for row in top20:
    print("(%s, %s) %d" % (row["word1"], row["word2"], row["count"]))

# Stop Spark and release resources.
spark.stop()
