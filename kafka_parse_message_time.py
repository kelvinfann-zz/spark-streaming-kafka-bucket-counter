#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark import SparkContext, AccumulatorParam
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from multiprocessing import Process, Queue

from kafka import SimpleProducer, KafkaClient

from audit_utils.utils import json_file_to_dict, avro_decoder_func, json_dict_bucket_parse, open_or_none, read_config_file, json_to_dict
from audit_utils.models import RecentArrayDumpTable, Switch, RecentSqlite3table
from audit_utils.http_endpoint import setup_site_RADT, set_site_sqlite

from flask import Flask

CONFIG_TYPES = {
	'KafkaSettings': {
		'brokers': str,
		'topic': str,
		'avro_val_schema': str,
	},
	'MsgSettings': {
		'bucket_interval': int,
		'bucket_field': str,
		'msg_map_schema': str,
		'bucket_type': str,
	},
	'HTTPEndpointSetting': {
		'sqlite_schema': str,
		'sqlite_db': str,
		'sqlite_table': str,
		'clean_interval': int,
		'clean_freq': int,
	},
}

DEFAULT_CONFIG = {
	'KafkaSettings': {
		'brokers': 'localhost:2181',
		'topic': 'logstash-test',
		'avro_val_schema': None,
	},
	'MsgSettings': {
		'bucket_interval': 20,
		'bucket_field': None,
		'msg_map_schema': None,
		'bucket_type': 'epoch',
	},
	'HTTPEndpointSetting': {
		'sqlite_schema': None,
		'sqlite_db': ':memory:',
		'sqlite_table': 'default',
		'clean_interval': 100,
		'clean_freq': 10,
	},
}

class AccumDict(AccumulatorParam):
	"""An Spark Accumulator used to keep track of 		
	"""
	def zero(self, initialValue):
		"""A zero dict should just have no entries
		"""
		return {}

	def addInPlace(self, v1, v2):
		for key in v2:
			if key not in v1:
				v1[key] = 0
			v1[key] += v2[key]
		return v1

def ss_kafka_interval_counter(zkQuorum, topic, bucket_interval, output_msg, 
		message_parse, valueDecoder=None):
	"""Starts a Spark Streaming job from a Kafka input and parses message time

	Args:
		brokers: the kafka broker that we look at for the topic
		topic: the kafka topic for input
		bucket_interval: the time interval in seconds (int) that the job will 
			bucket
		output_msg: a function that takes in a spark SparkContext (sc) and 
			StreamingContext (ssc) and returns a function that takes a rdd that 
			performs the output task

	Returns:
		None
		
	"""
	

	sc = SparkContext(appName="PythonKafkaParseMessage")
	ssc = StreamingContext(sc, bucket_interval + 5)

	if valueDecoder:
		kvs = KafkaUtils.createStream(
			ssc, zkQuorum, "spark-streaming-consumer", {topic: 1}, 
			valueDecoder=valueDecoder
		)
	else:
		kvs = KafkaUtils.createStream(
			ssc, zkQuorum, "spark-streaming-consumer", {topic: 1},
		)

	# I assume that we do not store kafka keys
	lines = kvs.map(lambda x: x[1])
	interval_counts = (lines.map(lambda line: (message_parse(line), 1))
							.reduceByKey(lambda a, b: a+b))

	output_msg_func = output_msg(sc, ssc)

	interval_counts.foreachRDD(output_msg_func)

	ssc.start()
	ssc.awaitTermination()

def ss_direct_kafka_interval_counter(brokers, topic, bucket_interval, 
		output_msg, message_parse, valueDecoder=None):
	"""Starts a Spark Streaming job from a Kafka input and parses message time

	WARNING!! This function only works for spark 1.4.0+ 

	Args:
		brokers: the kafka broker that we look at for the topic
		topic: the kafka topic for input
		timeinterval: the time interval in seconds (int) that the job will 
			bucket

	Returns:
		None
		
	"""
	sc = SparkContext(appName="PythonKafkaParseMessage")
	ssc = StreamingContext(sc, timeinterval + 5)

	if valueDecoder:
		kvs = KafkaUtils.createDirectStream(
			ssc, [topic], {"metadata.broker.list": brokers},
			valueDecoder=valueDecoder
		)
	else:
		kvs = KafkaUtils.createDirectStream(
			ssc, [topic], {"metadata.broker.list": brokers}
		)

	lines = kvs.map(lambda x: x[1])
	interval_counts = (lines.map(lambda line: (message_parse(line), 1))
							.reduceByKey(lambda a, b: a+b))

	output_msg_func = output_msg(sc, ssc)

	interval_counts.foreachRDD(output_msg_func)

	ssc.start()
	ssc.awaitTermination()

def combine_count_json(json_msg, count):
	"""Adds the count as a counts field in the json_msg 

	Args:
		json_msg: the string representation of the json msg
		count: the count for the count field that we will add

	Returns:
		a json message with the counts appended to it

	"""

	return '{0}, "count": {1}'.format(json_msg[:-1], count) + '}'

def create_send_kafka_msg_func(kafka_host, topic):
	"""Generates the function that takes in a stream of messages and sends them
	to the specified kafka sever/topic

	Args:
		kafka_host: the string of the kafka host location
		topic: the string name of the topic in the kafka host that the msgs are 
			sent

	Returns:
		A function that can take a iter of messages and sends them to the
		specified kafka server
		
	"""

	def send_kafka_msg(iters):
		#TODO: Add try/catch statements for kafka connection
		kafka = KafkaClient(kafka_host)
		producer = SimpleProducer(kafka)
		for key, val in iters:
			msg = combine_count_json(key, val)
			producer.send_messages(
				str(topic).encode('utf-8'), str(msg).encode('utf-8')
			)
		kafka.close()

	def per_rdd_do(rdd):
		rdd.foreachPartition(send_kafka_msg)

	return lambda sc, ssc: per_rdd_do

def create_http_share_func(mp_queue):
	"""Generates a func that writes the inputed values to a datastructure that 
	the HTTP

	Args:
		recent_storage: a RecentArrayDumpTable that stores the most recent 
			appended values
	Returns:
		A function that will populate the RecentArrayDumpTable given an iter
		
	"""

	def wrapper(sc, scc):
		"""The wrapper function
		"""
		accumOdd = sc.accumulator({}, AccumDict())
		accumEven = sc.accumulator({}, AccumDict())
		counter = Switch()

		def share_msg(iters, accum):
			"""What to do with each individual message
			"""
			for key, val in iters:
				accum.add({key:val})

		def per_rdd_do(rdd):
			"""What to do with each rdd
			"""

			if counter.value:
				to_use_accum = accumOdd
				other_accum = accumEven
			else:
				to_use_accum = accumEven
				other_accum = accumOdd

			counter.switch()

			share_msg_func = lambda iters: share_msg(iters, to_use_accum)

			rdd.foreachPartition(share_msg_func)
			#TODO: Make this into a generator
			add_to_q = []
			for key in to_use_accum.value:
				val = to_use_accum.value[key]
				json_dict = json_to_dict(combine_count_json(key, val))
				add_to_q.append(json_dict)
			mp_queue.put(add_to_q)
			other_accum.value.clear()

		return per_rdd_do

	return wrapper

def kafka_http_sqlite(brokers, topic, bucket_interval, conversion_dict, 
		bucket_field, bucket_type, avro_schema, sqlite_schema, db, table_name, 
		clean_interval, clean_freq_interval):
	"""Generates a func that writes the inputed values to a datastructure that 
	the HTTP

	Args:
		brokers: the broker (str) for the kafka topic (bellow)
		topic: Kafka topic (str) for the spark streaming job to listen to
		bucket_interval: an int of how frequent the spark streaming batch job 
			should run
	Returns:
		A function that will populate the RecentArrayDumpTable given an iter
		
	"""


	q = Queue()

	http_func = create_http_share_func(q)

	message_parse = lambda json_str: json_dict_bucket_parse(
		json_str, conversion_dict, bucket_field, bucket_interval, bucket_type
	)

	if avro_schema:
		value_decoder = avro_decoder_func(avro_schema)
	else:
		value_decoder = None

	s = lambda: ss_kafka_interval_counter(
		brokers, topic, bucket_interval, http_func, message_parse,
		valueDecoder=value_decoder
	)
	spark_streaming = Process(target=s)

	app = Flask(__name__)
	set_site_sqlite(app, q, sqlite_schema, db=db, table_name=table_name, 
		clean_interval=clean_interval, clean_freq_interval=clean_freq_interval
	)
	h = lambda: app.run(debug=False)
	http = Process(target=h)

	http.start()
	spark_streaming.start()

	http.join()
	spark_streaming.join()

def read_KHS_config_file(config_file_path):
	"""Reads a Kafka bucket counter HTTP Service config file and outputs all the
	params from the config_file
	"""
	config = read_config_file(config_file_path, DEFAULT_CONFIG)
	for header in CONFIG_TYPES:
		for key in CONFIG_TYPES[header]:
			cast = CONFIG_TYPES[header][key]
			if config[header][key]:
				config[header][key] = cast(config[header][key])
		
	brokers = config['KafkaSettings']['brokers']
	topic = config['KafkaSettings']['topic']
	bucket_interval = config['MsgSettings']['bucket_interval']
	bucket_field = config['MsgSettings']['bucket_field']
	bucket_type = config['MsgSettings']['bucket_type']
	db = config['HTTPEndpointSetting']['sqlite_db']
	table_name = config['HTTPEndpointSetting']['sqlite_table']
	clean_interval = config['HTTPEndpointSetting']['clean_interval']
	clean_freq_interval = config['HTTPEndpointSetting']['clean_freq']

	conversion_dict = json_file_to_dict(config['MsgSettings']['msg_map_schema'])
	avro_schema = open_or_none(config['KafkaSettings']['avro_val_schema'])
	sqlite_schema = json_file_to_dict(
		config['HTTPEndpointSetting']['sqlite_schema']
	)


	return (
		brokers, topic, bucket_interval, conversion_dict, bucket_field,
		bucket_type, avro_schema, sqlite_schema, db, table_name, clean_interval, 
		clean_freq_interval
	)

if __name__ == "__main__":
	"""Main function

	Usage: kafka_parse_message_time.py <config_file_path>
	
	"""
	if len(sys.argv) == 2:
		config_file_path = sys.argv[1:]
		kafka_http_sqlite(*read_KHS_config_file(config_file_path))

	else:
		print >> sys.stderr, ("kafka_parse_message_time.py <config_file_path>")
		exit(-1)


