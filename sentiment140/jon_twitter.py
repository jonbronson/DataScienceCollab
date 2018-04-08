from pyspark import SparkContext
from pyspark.sql.session import SparkSession


sc = SparkContext("local", "Twitter")
spark = SparkSession(sc)

filename = "training.1600000.processed.noemoticon.csv";

df = spark.read.format("csv").option("header", "false").load(filename);

df.write.parquet("twitter_training.parquet")

