import datetime
import io
import avro.schema
import avro.io
try:
    import json
except ImportError:
    import simplejson as json

from ConfigParser import ConfigParser

EPOCH_ORIG=datetime.datetime(1970,1,1)

LEGEND={
	'epoch': (lambda epoch, interval: bucket_epoch_epoch(
		interval, long(epoch))
	), 
	'iso': (lambda iso, interval: bucket_datetime_epoch(
		interval, cast_iso_to_dtime(iso))
	),
}

def json_dict_parse(json_str, conversion_dict, ensure_ascii=False):
	"""General json parse that converts a json per conversion_dict specificati-
	ons. 

	Args:
		json_str: a string representation of the json message that needs to be
			transformed. Assumes the json_str is a json dictionary
		conversion_dict: a dictionary of the items to be parsed. It is in the
			the format..
			{
				'new_key': 'json_str_key',
				'new_key2': 'json_str_key2',
				'new_key3': 'json_str_key3',
			}
			The keys for the dictionary will be the new keys for the transform-
			ed json and the values associated with each key indicate the key 
			from the original json whose value will now be associated with the
			new keys. return_json[new_key]=json_str[json_str_key]. See doctest
			for example

	Returns:
		A new dictionary object parsed with the objects specified. 

	Examples:
		>>> json_str = '{"offset": "1"}'
		>>> conversion_dict = {"old_offset": "offset"}
		>>> json_dict_parse(json_str, conversion_dict, True)
		'{"old_offset": "1"}'

	"""
	json_dict = json.loads(json_str)

	new_json = {}
	for new_key in conversion_dict:
		json_dict_key = conversion_dict[new_key]
		new_json[new_key] = json_dict[json_dict_key]

	return json.dumps(new_json, ensure_ascii=ensure_ascii)

def json_dict_bucket_parse(json_str, conversion_dict, bucket_field, 
	bucket_interval, bucket_type, ensure_ascii=False):
	"""Parses a json based on the `conversion_dict` and adds two fields 
	`bucket_start` and `bucket_end` based on the `bucket_field`

	Args:
		json_str: a string representation of the json message that needs to be
			transformed. Assumes the json_str is a json dictionary
		conversion_dict: a dictionary of the items to be parsed. It is in the
			the format..
			{
				'new_key': 'json_str_key',
				'new_key2': 'json_str_key2',
				'new_key3': 'json_str_key3',
			}
			The keys for the dictionary will be the new keys for the transform-
			ed json and the values associated with each key indicate the key 
			from the original json whose value will now be associated with the
			new keys. return_json[new_key]=json_str[json_str_key]. See doctest
			for example
		bucket_field: an epoch (int) field within the json string that will be
			used to for the buckets

	Returns:
		A new dictionary object parsed with the objects specified and  

	"""
	new_json = json_dict_parse(json_str, conversion_dict)
	new_json_dict = json.loads(new_json)
	old_json_dict = json.loads(json_str)

	cast = LEGEND[bucket_type]
	bucket_start, bucket_end = cast(old_json_dict[bucket_field],bucket_interval)
	new_json_dict['bucket_start'] = bucket_start
	new_json_dict['bucket_end'] = bucket_end
	return json.dumps(new_json_dict, ensure_ascii=ensure_ascii)	

def bucket_time(interval, dtime):
	"""Returns the time interval (in seconds) that the dtime belongs to

	Args:
		interval: the number of seconds (int) between each time interval. We 
		dtime: a datetime.time object that we are trying to get the bucket for

	Returns:
		An int representation of the timeinterval the dtime belongs to (in 
		seconds)
		
	"""

	sec_since_day = dtime.second + 60*(dtime.minute + 60*dtime.hour)
	return (sec_since_day / interval) * interval

def sec_to_HMS(sec):
	"""Returns the hours, minutes, seconds in the input sec

	Args:
		sec: number of seconds (int) that need to be converted

	Returns:
		tuple(hours, minutes, seconds) 
		
	"""
	hours = sec / 3600
	sec = sec % 3600
	minutes = sec / 60
	sec = sec % 60

	return hours, minutes, sec

def bucket_datetime(interval, dtime):
	"""Returns the time interval (in seconds) that the dtime belongs to

	Args:
		interval: the number of seconds (int) between each time interval.  
		dtime: a datetime.time object that we are trying to get the bucket for

	Returns:
		An int representation of the timeinterval the dtime belongs to (in
		seconds)
		
	"""

	bucket = bucket_time(interval, dtime)
	bucket_hour, bucket_min, bucket_sec = sec_to_HMS(bucket)

	b_start = dtime.replace(
		hour=bucket_hour,
		minute=bucket_min, 
		second=bucket_sec,
		microsecond=0
	)
	b_end = b_start + datetime.timedelta(seconds=interval)



	start_str = "{0}-{1}-{2}T{3}:{4}:{5}.0Z".format(
		dtime.year, dtime.month, dtime.day, bucket_hour, bucket_min, bucket_sec
	)
	end_str = "{0}-{1}-{2}T{3}:{4}:{5}.0Z".format(
		b_end.year, b_end.month, b_end.day,
		b_end.hour, b_end.minute, b_end.second
	)

	return start_str, end_str

def bucket_now_epoch(interval=20):
	"""Returns the time interval (in seconds) that the current time belongs to 
	in epoch
	form

	Args:
		interval: the number of seconds (int) between each time interval. 

	Returns:
		An epoch representation of the timeinterval the the current time belongs 
		to (in seconds)
		
	"""
	now = datetime.datetime.utcnow()
	return bucket_datetime_epoch(interval, now)

def bucket_datetime_epoch(interval, dtime):
	"""Returns the time interval (in seconds) that the dtime belongs to in epoch
	form

	Args:
		interval: the number of seconds (int) between each time interval.  
		dtime: a datetime.time object that we are trying to get the bucket for

	Returns:
		An epoch representation of the timeinterval the dtime belongs to (in 
		seconds)

	Example:
		>>> dt = EPOCH_ORIG
		>>> bucket_datetime_epoch(10, dt)
		(0L, 10L)
		>>> dt = datetime.datetime(2015, 8, 19, 18, 40, 2, 177979)
		>>> bucket_datetime_epoch(10, dt)
		(1440009600L, 1440009610L)
		
	"""

	bucket = bucket_time(interval, dtime)
	# bucket_hour, bucket_min, bucket_sec = sec_to_HMS(bucket)

	b_start = dtime.replace(hour=0, minute=0, second=0, microsecond=0)

	start_interval = long((b_start - EPOCH_ORIG).total_seconds()) + bucket
	end_interval = start_interval + interval

	return start_interval, end_interval

def bucket_epoch_epoch(interval, epoch):
	"""Buckets an epoch time into an epoch time bucket

	Args:
		interval: the number of seconds (int) between each time interval. 
		epoch: the epoch time (int) that needs to be bucketed

	Returns:
		An epoch time bucket (int)

	Example:
		>>> bucket_epoch_epoch(10, 1)
		(0L, 10L)
		>>> bucket_epoch_epoch(10, 9)
		(0L, 10L)
		>>> bucket_epoch_epoch(10, 11)
		(10L, 20L)

	"""
	bucket_start = long(epoch / interval) * interval
	bucket_end = bucket_start + interval

	return bucket_start, bucket_end

def cast_iso_to_dtime(iso_time):
	"""Casts a iso_time string into a datetime.time object

	Args:
		iso_time: a string representation of time in ISO format 
			(%Y-%m-%dT%H:%M:%S.%fZ)

	Returns:
		A datetime.time object representation of the string		
	"""
	return datetime.datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%S.%fZ")

def json_to_dict(json_str):
	"""Converts a json to a string. Created to avoid importing the json library 
	again

	Args:
		json_str: the json dict string that needs to be parsed
		
	Returns:
		A dictionary from the json string

	"""
	return json.loads(json_str)

def json_file_to_dict(json_file_path):
	"""Converts a json file into a dictionary

	Args:
		json_file_path: the path to a json file
		
	Returns:
		A dictionary form of the json
	"""
	return json.load(open(json_file_path))

def dict_to_avro(schema, dict_iter):
	"""Converts a dict to binary string

	Args:
		schema: the avro schema (str)
		dict_iter: an iterable of dictionaries that follow the schema that are 
			to be converted into an avro binary string
		
	Returns:
		The number of messages converted to avro (len(dict_iter)) and the binary
		string of the avro message

	Examples:
		See `avro_to_str`
	"""

	avro_schema = avro.schema.parse(schema)
	writer = avro.io.DatumWriter(avro_schema)

	bytes_writer = io.BytesIO()
	encoder = avro.io.BinaryEncoder(bytes_writer)
	msg_counter = 0
	for avro_dict in dict_iter:
		writer.write(avro_dict, encoder)
		msg_counter += 1

	raw_bytes = bytes_writer.getvalue()

	return msg_counter, raw_bytes

def avro_to_dict(schema, msg_count, raw_bytes):
	"""Converts avro binary string to readable strings

	Args:
		schema: the avro schema (str)
		msg_count: the number of messages in the raw_bytes
		raw_bytes: a bianry string of the avro message that needs to be 
		
	Returns:
		The number of messages converted to avro (len(dict_iter)) and a list of
		the messages in the form of strings

	Examples:
		>>> schema = '{"namespace": "example.avro", "type": "record", "name":
		... 		"User", "fields": [{"name": "name", "type": "string"}, {"name":
		... 		"favorite_number",  "type": ["int", "null"]}, {"name":
		... 		"favorite_color", "type": ["string", "null"]}]}'
		>>> dict_iter = [{"name": "Alyssa", "favorite_number": 256},]
		>>> count, raw_bytes = dict_to_avro(schema, dict_iter)
		>>> s = avro_to_dict(schema, count, raw_bytes)
		>>> s
		[{u'favorite_color': None, u'favorite_number': 256, u'name': u'Alyssa'}]
		>>> type(s[0]) is dict
		True

	"""
	avro_schema = avro.schema.parse(schema)
	bytes_reader = io.BytesIO(raw_bytes)
	decoder = avro.io.BinaryDecoder(bytes_reader)
	reader = avro.io.DatumReader(avro_schema)
	dict_list = []
	for _ in xrange(msg_count):
		dict_list.append(reader.read(decoder))
	return dict_list

def avro_decoder_func(schema):
	"""Creates an avro decoder function

	Args:
		schema: the avro schema (str)
		
	Returns:
		A function that will take in a single raw avro msg and decode it into a
		string form of the json 

	Example:
		>>> test_schema = '''{"namespace": "example.avro","type": "record",
		...		"name": "User","fields": [{"name": "name", "type": "string"},
		...		{"name": "favorite_number",  "type": ["int", "null"]},{"name":
		...		"favorite_color", "type": ["string", "null"]}]}'''
		>>> decoder = avro_decoder_func(test_schema)
		>>> schema = avro.schema.parse(test_schema)
		>>> writer = avro.io.DatumWriter(schema)
		>>> bytes_writer = io.BytesIO()
		>>> encoder = avro.io.BinaryEncoder(bytes_writer)
		>>> writer.write({"name": "Alyssa", "favorite_number": 256}, encoder)
		>>> raw_bytes = bytes_writer.getvalue()
		>>> decoder(raw_bytes)
		'{"favorite_color": null, "favorite_number": 256, "name": "Alyssa"}'

	"""
	avro_schema = avro.schema.parse(schema)
	reader = avro.io.DatumReader(avro_schema)
	def avro_decoder(avro_msg_raw):
		bytes_reader = io.BytesIO(avro_msg_raw)
		decoder = avro.io.BinaryDecoder(bytes_reader)
		return avro_msg_raw and json.dumps(reader.read(decoder))
	return avro_decoder

def open_or_none(file_path):
	"""
	"""
	if file_path == 'None' or not file_path:
		return None
	else:
		return open(file_path).read()

def read_config_file(config_file_path, config_default=dict()):
	"""Parses a config file into a python dictionary
	"""
	conf = ConfigParser()
	conf.read(config_file_path)
	config = config_default
	for section in conf.sections():
		if section not in config:
			config[section] = dict()
		for key, val in conf.items(section):
			config[section][key] = val
	return config

if __name__ == "__main__":
	import doctest
	doctest.testmod()

