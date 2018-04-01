from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import tensorflow as tf
import pandas as pd
import time
import tempfile

sc = SparkContext("local", "Simple App")
spark = SparkSession(sc)

spark_df = spark.read.parquet('chocolate_ratings_augmented.parquet')
df = spark_df.toPandas();


# https://www.tensorflow.org/versions/r1.2/tutorials/wide_and_deep
COLUMNS = ['Company', 'SpecificBeanOriginOrBarName', 'CocoaPercent', 'CompanyLocation', 'Rating', 'BeanType', 'BroadBeanOrigin']
CATEGORICAL_COLUMNS = ['Company', 'SpecificBeanOriginOrBarName', 'CompanyLocation', 'BeanType', 'BroadBeanOrigin']

df['RatingLabel'] = [int(x<=2.5) for x in df['Rating']]
LABEL_COLUMN = 'RatingLabel'

company = tf.feature_column.categorical_column_with_hash_bucket('Company', hash_bucket_size=1000)
specific_origin = tf.feature_column.categorical_column_with_hash_bucket('SpecificBeanOriginOrBarName', hash_bucket_size=1000)
company_location = tf.feature_column.categorical_column_with_hash_bucket('CompanyLocation', hash_bucket_size=1000)
bean_type = tf.feature_column.categorical_column_with_hash_bucket('BeanType', hash_bucket_size=1000)
broad_origin = tf.feature_column.categorical_column_with_hash_bucket('BroadBeanOrigin', hash_bucket_size=1000)
CONTINUOUS_COLUMNS = ['CocoaPercent']
cocoa_percent = tf.contrib.layers.real_valued_column('CocoaPercent')

# use pandas functions to split data into training and test sets
df_train = df.sample(frac=0.8, random_state=int(time.time()))

# df.index: row labels
df_test = df.drop(df_train.index)

def input_fn(df):
    continuous_cols = {k: tf.constant(df[k].values) for k in CONTINUOUS_COLUMNS}
    categorical_cols = {k: tf.SparseTensor(
        indices=[[i, 0] for i in range(df[k].size)],
        values=df[k].values,
        dense_shape=[df[k].size, 1]) for k in CATEGORICAL_COLUMNS}

    # only works for Python 3.5 and newer
    feature_cols = {**continuous_cols, **categorical_cols}
    print(feature_cols)
    label = tf.constant(df[LABEL_COLUMN].values)
    return feature_cols, label

def train_input_fn():
    return input_fn(df_train)

def eval_input_fn():
    return input_fn(df_test)

model_dir = tempfile.mkdtemp()

steps = 1000
e = tf.contrib.learn.LinearClassifier(model_dir=model_dir, feature_columns=[bean_type, cocoa_percent])
e.fit(input_fn=train_input_fn, steps=steps)
results = e.evaluate(input_fn=eval_input_fn, steps=steps)
print(results)
