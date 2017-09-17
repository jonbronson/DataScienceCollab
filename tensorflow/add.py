import tensorflow as tf
import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='2'

session = tf.Session()

node_pi = tf.constant(3.14159)
node_two = tf.constant(2.0);

node_sum = tf.add(node_pi, node_two);

print(session.run(node_sum));
