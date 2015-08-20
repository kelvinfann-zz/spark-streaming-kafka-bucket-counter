#Spark Streaming Bucket Counter with HTTP Endpoint

This is a spark streaming job with an HTTP RESTful-like Endpoint for querying the results of the batch jobs in the spark streaming. The HTTP Endpoint store the spark streaming job's results in a SQLite database so the HTTP calls support full SQL calls.

##Dependencies

Nonstandard Python Libraries
- [Flask](http://flask.pocoo.org)
- [Flask-RESTful](https://flask-restful.readthedocs.org/en/0.3.4/)
- [Avro](http://avro.apache.org)
- [Python-Kafka](https://github.com/mumrah/kafka-python)

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
sqlite_db = path to sqlite db file that will be used as the backend for the HTTP endpoint. Or ':memory:' [NOTE that if you choose a NON-memory database you must make a reconnect call after the HTTP service starts. ]
sqlite_table = sqlite table name in which the HTTP endpoint will store data to
clean_interval = 'how long' the message counts will be stored in the sqlite table
clean_freq = 'how frequent' the messages will be cleaned
```

Example:
```ini
[KafkaSettings]
brokers = localhost:2181
topic = test
avro_val_schema = /path/to/avro/schema.avsc

[MsgSettings]
bucket_interval = 20
bucket_field = timestamp
bucket_type = epoch
msg_map_schema = /path/to/msg/map/schema.json

[HTTPEndpointSetting]
sqlite_schema = /path/to/sqlite/schema.json
sqlite_db = :memory:
sqlite_table = default
clean_interval = 10
clean_freq = 10


```

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

