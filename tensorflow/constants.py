import tensorflow as tf
import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='2'

session = tf.Session()

node_pi = tf.constant(3.14159)

print(session.run(node_pi));
