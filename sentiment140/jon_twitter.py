from pyspark import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import UserDefinedFunction
from email.utils import parsedate_to_datetime

sc = SparkContext("local", "Twitter")
spark = SparkSession(sc)

filename = "training.1600000.processed.noemoticon.csv"

df = spark.read.format("csv").option("header", "false").load(filename)

df = df.withColumn("polarity", df._c0.cast(DoubleType())).drop("_c0")
df = df.withColumn("tweetId", df._c1.cast(IntegerType())).drop("_c1")
df = df.withColumnRenamed('_c2', 'date')
df = df.withColumnRenamed('_c3', 'query')
df = df.withColumnRenamed('_c4', 'user')
df = df.withColumnRenamed('_c5', 'text')
df = df.select(["tweetId", "date", "user", "text", "polarity", "query"])
df = df.withColumn('date', UserDefinedFunction(lambda date: int(parsedate_to_datetime(date).timestamp()), IntegerType())('date'))

df.write.parquet("twitter_training.parquet")


