#Spark Streaming Bucket Counter with HTTP Endpoint

This is a spark streaming job with an HTTP RESTful-like Endpoint for querying the results of the batch jobs in the spark streaming. The HTTP Endpoint store the spark streaming job's results in a SQLite database so the HTTP calls support full SQL calls.

Note this application assumes that the messages on the kafka server are in json form or are in avro that, when uncoverted are also json form.

##Dependencies

Spark Dependencies

- [Spark](http://spark.apache.org/) (1.3.0+)

Nonstandard Python Libraries

- [Flask](http://flask.pocoo.org)
- [Flask-RESTful](https://flask-restful.readthedocs.org/en/0.3.4/)
- [Avro](http://avro.apache.org)
- [Python-Kafka](https://github.com/mumrah/kafka-python)
- [MysqlDB](https://pypi.python.org/pypi/MySQL-python/)

The audit_utils is also require and is included in this repo. 
Simply `cd audit_utils` and `sudo python setup.py install`

##Running

1. Install all dependencies (For audit_utils follow the instructions above)
2. Setup the config file. See example bellows
3. Running: as with all spark jobs, use `spark-submit`. You do need to include the spark-streaming-kafka package, however which can be done by including, `--packages org.apache.spark:spark-streaming-kafka_2.10:1.3.0` or alternatively you can download the maven .jar from [maven](http://search.maven.org/#search|ga|1|a%3A%22spark-streaming-kafka-assembly_2.10%22%20AND%20v%3A%221.3.0%22) and include it after the `--jars` flag. The application itself requires just the path to the config file as a param. `kafka_parse_message_time.py <config_file_path>`

Run command ex:
```
bin/spark-submit --jars spark-streaming-kafka-assembly_2.10-1.3.0.jar --master local[*] kafka_parse_message_time.py /Users/kelvin/configs/logstash_spark.ini;;
```

##Config File

```
[KafkaSettings]
brokers = Kafka Broker 
topic = Kafka Topic
avro_val_schema = path to .avsc avro schema file or None

[MsgSettings]
bucket_interval = How many seconds allocated per timebucket. [NOTE: Must evenly divide into the total seconds in a day]
bucket_field = The field in the kafka messages that will be parsed into the timebucket
bucket_type = The format the bucket_field is in [NOTE: currently supports 'epoch' or 'iso']
msg_map_schema = path to .json msg map schema file

[HTTPEndpointSetting]
sqlite_schema = path to .json sqlite schema file
sqlite_db = path to sqlite db file that will be used as the backend for the HTTP endpoint. Or ':memory:' [NOTE that if you choose a NON-memory database you must make a reconnect call after the HTTP service starts (’/r’). ]
sqlite_table = sqlite table name in which the HTTP endpoint will store data to
clean_interval = 'how long' the message counts will be stored in the sqlite table
clean_freq = 'how frequent' the messages will be cleaned
```

##HTTP Endpoint Urls

- '/a/<string:table_name>': Select all data from the sqlite database given the table_name you specified
- '/c/<string:custom>': Runs the 'custom' string as a query against the Sqlite db
- '/r': Resets the sqlite db connection. Use this if you are connecting to a non-inmemory db only
- '/rst': the current RST_ID that the HTTP service is on

##Example

Config:

```
[KafkaSettings]
brokers = localhost:2181
topic = test
avro_val_schema = None

[MsgSettings]
bucket_interval = 20
bucket_field = timestamp
bucket_type = epoch
msg_map_schema = /path/to/msg/map/msg_map_schema.json

[HTTPEndpointSetting]
sqlite_schema = /path/to/sqlite/sqlite_schema.json
sqlite_db = :memory:
sqlite_table = default
clean_interval = 10
clean_freq = 10
```

msg_map_schema.json:
[Note] this is where you specify how you want to group the messages in kafka for the count and what you want the fields to show up as in the sqlite database

```
{
	"origin_server":"server_name",
	"topic":"topic"
}
```

sqlite_schema.json:

```
{
	"server_name":"TEXT",
	"topic":"TEXT",
	"bucket_start":"INTEGER",
	"bucket_end":"INTEGER",
	"count":"INTEGER"
}
```

Kafka Message Input:

```
{
	"origin_server": "Iceman",
	"topic": "Dreams"
}
```

HTTP Output:

`curl -X GET localhost:5000/a/default`

```
[{
	"count": 1,
	"bucket_start": 1440179080,
	"bucket_end": 1440179100, 
	"RST_ID": 1,
	"topic": "Dreams",
	"server_name": "Iceman",
}]
```

`curl -X GET localhost:5000/c/Select * FROM default WHERE count=1`

```
[{
	"count": 1,
	"bucket_start": 1440179080,
	"bucket_end": 1440179100, 
	"RST_ID": 1,
	"topic": "Dreams",
	"server_name": "Iceman",
}]
```

`curl -X GET localhost:5000/c/Select * FROM default WHERE count=0`

```
[]
```

`curl -X GET localhost:5000/rst`

```
2
```

`curl -X GET localhost:5000/r`

```
nil
```

[Note] Everything resets because it is an inmemory db

`curl -X GET localhost:5000/rst`

```
1
```

`curl -X GET localhost:5000/a/default`

```
[]
```




Note that the RST_ID is just used for cleaning purposes in the sqlite database. It is just an incrementing ID indicating which spark stream the data was written from. You do not need to specify the RST_ID field in the sqlite schema

##Deploying Mysql HTTP Endpoint

To deploy the flask http endpoint please follow the flask documentation. http://flask.pocoo.org/docs/0.10/deploying/ All the code for the flask app is just in the mysql_http.py file

##Design Overview

There are two main portions of this app:

- Spark Streaming Job
- HTTP Endpoint

The two main processes are linked by a shared data queue in which the spark streaming job publishes data into, and the HTTP endpoint consumes that data and stores it for a finite period of time.

The general flow of this app after the app is started is:

1. Some Publisher publishes a message to Kafka 
2. Spark Streaming listens into kafka and consumes new data as it comes in
3. Spark Streaming processes the data
4. Spark Streaming posts processed data onto a shared queue
5. HTTP Endpoint is queried at some point and it consumes all the data on the shared queue
6. HTTP Endpoint stores the consumed data for a finate period of iterations
7. Some consumer periodically makes HTTP calls to fetch the data stored

