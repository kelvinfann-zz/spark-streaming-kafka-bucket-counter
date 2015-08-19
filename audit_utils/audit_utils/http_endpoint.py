from flask_restful import Api, Resource

from .utils import json_to_dict, dict_to_avro
from .models import RecentSqlite3table

class HttpEndpoint(Resource):
	"""Essentially the parent class of all the other Flask RESTful class. 
	Attaches a datastore to each of the RESTful classes to query from.
	"""
	@classmethod
	def set(cls, recent_storage, avro_schema=None):
		cls.recent_storage = recent_storage
		cls.avro_schema = avro_schema
		return cls

##########################################
#RecentArrayDumpTable Datastore Endpoint##
##########################################

def setup_site_RADT(app, recent_storage, data_queue):
	"""A setup function that sets up the site's calls for the http endpoint.
	Uses the Recent Array Dump-Table to store the data in memory
	"""
	api = Api(app)

	updatevals = lambda: recent_storage.queue_append(data_queue)
	app.before_request(updatevals)

	recentval = RADTRecentVal.set(recent_storage)
	directval = RADTDirectVal.set(recent_storage)
	selectrange = RADTSelectRange.set(recent_storage)
	custom = RADTCustom.set(recent_storage)

	api.add_resource(recentval, '/rv/<int:num_items>')
	api.add_resource(directval, '/dv/<int:index>')
	api.add_resource(selectrange, '/sr/<string:parameter>/<string:range_start>:<string:range_end>')
	api.add_resource(custom, '/c/<string:c_index>/EOE')

class RADTRecentVal(HttpEndpoint):
	"""An http get call that will return the `num_items` most recent items 
	stored into the `recent_storage`
	"""

	def get(self, num_items):
		return self.recent_storage.get_recent(num_items)

class RADTDirectVal(HttpEndpoint):
	"""An http get call that will return the `index`th item in the 
	`recent_storage`
	"""

	def get(self, index):
		return self.recent_storage.get(index)

class RADTSelectRange(HttpEndpoint):
	"""An http get call that will all row entries in the specified range
	"""

	def get(self, parameter, range_start, range_end):
		if range_start != 'None':
			range_start = int(range_start)
		else:
			range_start = None
		if range_end != 'None':
			range_end = int(range_end)
		else:
			range_end = None
		search = {parameter: (range_start, range_end)}
		return self.recent_storage.select_range(search)

class RADTCustom(HttpEndpoint):
	"""An http get call that will all row entries in the specified range
	"""

	def get(self, c_index):
		s = {}
		try:
			s = json_to_dict(c_index)
			return self.recent_storage.c_general_select(s)			
		except:
			return self.recent_storage.c_general_select({})

##########################################
##SQLite Datastore Endpoint###############
##########################################

def set_site_sqlite(app, data_queue, sql_schema, db=None, table_name=None,
	clean_interval=100, clean_freq_interval=10):
	"""Sets up a FlaskRestful HTTP endpoint for a SQLite database given a Flask
	App.

	Args:
		app: the Flask service object that is being enhanced into a RESTful 
			endpoint
		data_queue: a queue used as the source of data for the Flask Endpoint.
			Each item in the queue should be an iterable filled with dicts. The 
			SQLite database will read from this queue aperiodically and append 
			all new objects
		sql_schema: the sqlite schema that is to be used for the SQLite
		db: the database path (str) that will be used as the SQLite 
		table_name: the table name in the database we will be using to store the
			data. The table must:
				a) Not exist yet
				b) Have the same schema as specified in the `sql_schema` param
		clean_interval: the number of recent bulk records (int) that will be 
			kept on the Sqlite database. A bulk record is essentially an object
			(iter of rows (dicts)) from the data_queue
		clean_freq_interval: the frequency (int) in which terms of bulk records
			that the service will check to clean records

	Returns:
		None
		
	"""
	api = Api(app)

	sqlite_obj = RecentSqlite3table(sql_schema, db=db, table_name=table_name,
		clean_interval=clean_interval, clean_freq_interval=clean_freq_interval
	)

	updatevals = lambda: sqlite_obj.queue_insert(data_queue)
	app.before_request(updatevals)

	select_all = SLSelectAll.set(sqlite_obj)
	custom_select = SLCustomSelect.set(sqlite_obj)
	custom = SLCustom.set(sqlite_obj)
	reconnect = SLReconnect.set(sqlite_obj)
	rstid_fetch = RSTIDFetch.set(sqlite_obj)

	api.add_resource(select_all, '/a/<string:table_name>')
	api.add_resource(custom, '/c/<string:custom>')
	api.add_resource(custom_select, '/cs/<string:custom_select>')
	api.add_resource(reconnect, '/r')
	api.add_resource(rstid_fetch, '/rst')
		
class SLSelectAll(HttpEndpoint):
	"""An http get call that returns all the current 
	"""
	def get(self, table_name):
		if table_name == '*':
			return self.recent_storage.select_all()
		else:
			return self.recent_storage.select_all(table_name)

class SLCustomSelect(HttpEndpoint):
	"""An http get call returns a custom SELECT statement
	"""
	def get(self, custom_select):
		base_custom = self.recent_storage.run_cmd(custom_select)
		if self.avro_schema:
			return dict_to_avro(self.avro_schema, base_custom)
		else:
			return base_custom

class SLCustom(HttpEndpoint):
	"""An http get call returns a custom statement. Cannot return in avro format
	"""
	def get(self, custom):
		return self.recent_storage.run_cmd(custom)

class SLReconnect(HttpEndpoint):
	"""An http get call that returns all the current 
	"""
	def get(self):
		self.recent_storage.reconnect()

class RSTIDFetch(HttpEndpoint):
	"""An http get call that returns all the current 
	"""
	def get(self):
		return self.recent_storage.rst_id