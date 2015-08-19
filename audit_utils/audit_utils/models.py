import sqlite3
import sys


DEFAULT_TABLE_NAME='default'
MEM_DB=':memory:'

class Sqlite3Table(object):
	"""A Sqlite Database wrapper object

	Args:
		sql_schema: the sqlite schema that is to be used for the SQLite. Follows
			the following format:
			{
				'column_name': 'sqlite_type',
				'column_name2': 'sqlite_type2',
				...
				'column_nameN': 'sqlite_typeN',
			}
			For sqlite types please see Sqlite's documentation
		db: the database path (str) that will be used as the SQLite 
		table_name: the table name in the database we will be using to store
			the data. The table must:
				a) Not exist yet
				b) Have the same schema as specified in the `sql_schema` 
				param
		clean_interval: the number of recent `sudobulk_insert` items (int) 
			that will be kept on the Sqlite database.
		clean_freq_interval: the frequency (int) in which terms of 
			`sudobulk_insert` that the service will check to clean records

	Attributes:
		schema: the sqlite schema that is to be used for the SQLite
		db_name: the database path (str) that will be used as the SQLite 
		db: the sqlite3 connection 
		cur: the sqlite3 cursor
		is_memory: a boolean indicating if the database is in memory
		default_table: if a table_name is left off in any method calls, the sql
			cmds will be executed with this `default_table` name

	Examples:
		>>> schema = {'num':'INT', 'name':'TEXT'}
		>>> s = Sqlite3Table(schema)
		>>> _ = s.cur.execute('SELECT * FROM sqlite_master')
		>>> s.cur.fetchall()[0][4]
		u'CREATE TABLE "default"(num INT, name TEXT)'
			
	"""

	def __init__(self, sql_schema, db=None, table_name=None):
		if not db:
			db = MEM_DB
		self.schema = sql_schema
		self.db_name = db
		self.db = sqlite3.connect(db, check_same_thread=False)
		self.cur = self.db.cursor()
		self.is_memory = db == MEM_DB
		if not table_name:
			self.default_table = DEFAULT_TABLE_NAME
		else:
			self.default_table = table_name
		if self.is_memory:
			self.create_table(self.default_table)
		elif not self.check_table(table_name):
			self.create_table(table_name)
			self.commit()

	def run_cmd(self, cmd):
		"""Runs a generic comand on the sqlite3 database

		Args:
			cmd: a string of the SQL command to be run against the Sqlite DB

		Returns:
			The `fetchall` results of the cmd in a dict with the `describe`
			results added as keys

		Examples:
			>>> s = Sqlite3Table({'num':'INT'})
			>>> s.run_cmd('')
			[]
			>>> r = s.run_cmd('SELECT * FROM sqlite_master')
			>>> len(r)
			1
			>>> r[0]['sql']
			u'CREATE TABLE "default"(num INT)'

		"""
		try:
			self.cur.execute(cmd)
		except:
			print >> sys.stderr, 'Bad cmd: {0}'.format(cmd)
			raise
		return_list = []
		for row in self.cur.fetchall():
			new_dict = {}
			for index in xrange(len(self.cur.description)):
				new_dict[self.cur.description[index][0]] = row[index]
			return_list.append(new_dict)
		return return_list

	def attach_new_db(self, db_path, db_name):
		"""Attaches a database to current one

		Args:
			db_path: a string of the db that is to be attached
			db_name: a string of the name of the db will be labeled as within 
				the sqlite_master
			
		Returns:
			A boolean indicating whether the attach worked

		Examples:
			>>> s = Sqlite3Table({'num':'INT'})
			>>> r = s.run_cmd('PRAGMA database_list')
			>>> len(r)
			1
			>>> s.attach_new_db(':memory:', 'mem1')
			True
			>>> s.attach_new_db(':memory:', 'mem1')
			False
			>>> s.attach_new_db(':memory:', 'mem2')
			True
			>>> r = s.run_cmd('PRAGMA database_list')
			>>> len(r)
			3

		"""	

		attach_str = 'ATTACH DATABASE "{0}" as "{1}"'
		attach_cmd = attach_str.format(db_path, db_name)
		try:
			self.run_cmd(attach_cmd)
			return True
		except:
			print >> sys.stderr, 'Unable to attach db: {0}'.format(db_name)
			return False

	def check_table(self, table_name=None):
		"""Checks if a table exhists in the db

		Args:
			table_name: a string of the table name. Defaults to self.
			
		Returns:
			A boolean indiciating if the table exhists. Returns True if so

		Examples:
			>>> s = Sqlite3Table({'num':'INT'})
			>>> s.check_table()
			True
			>>> s.check_table('nonexhisting_table')
			False

		"""	
		if not table_name:
			table_name = self.default_table
		check_str = (
			'SELECT name FROM sqlite_master '
			'WHERE type="table" AND name="{0}"'
		)
		check_cmd = check_str.format(table_name)
		return len(self.run_cmd(check_cmd)) > 0

	def create_table(self, table_name, schema=None):
		"""Creates a table in the database

		Args:
			table_name: a str of the table name 
			schema: a dictionary schema used for the table
			
		Returns:
			A boolean indiciating if the table exists. Returns True if the table
			exhists; else returns False

		Examples:
			>>> s = Sqlite3Table({'num':'INT', 'name':'TEXT'})
			>>> s.create_table('nondefault')
			[]
			>>> s.describe_table('nondefault')
			u'(num INT, name TEXT)'

		"""
		if self.check_table(table_name):
			raise ValueError('Bad table name, {0} already exists')
		create_str = 'CREATE TABLE "{0}"({1})'
		if not schema:
			schema = self.schema
		table_schema_str = ', '.join(
			'{0} {1}'.format(key, schema[key]) for key in schema
		)
		create_table_cmd = create_str.format(table_name, table_schema_str)
		return self.run_cmd(create_table_cmd)

	def describe_table(self, table_name=None):
		"""Gets the creation statement for the table

		Args:
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			
		Returns:
			A boolean indiciating if the table exists. Returns True if the table
			exhists; else returns False

		Examples:
			>>> s = Sqlite3Table({'num':'INT', 'name':'TEXT'})
			>>> s.describe_table()
			u'(num INT, name TEXT)'

		"""
		if not table_name:
			table_name = self.default_table
		pragma_str = (
			'SELECT * FROM sqlite_master '
			'WHERE type="table" AND name="{0}"'
		)
		pragma_cmd = pragma_str.format(table_name)
		create_str = self.run_cmd(pragma_cmd)[0]['sql']
		return create_str[create_str.index('('):]
	
	def select_all(self, table_name=None):
		"""Lists all entries in a specified table

		Args:
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			
		Returns:
			A list of dictionarys where each dictionary is a row in the table

		Examples:
			>>> s = Sqlite3Table({'num':'INT', 'name':'TEXT'})
			>>> s.select_all()
			[]
			>>> s.insert({'num':'1', 'name':'one'})
			True
			>>> s.select_all()
			[{'num': 1, 'name': u'one'}]

		"""
		if not table_name:
			table_name = self.default_table
		select_str = 'SELECT * FROM "{0}"'
		select_cmd = select_str.format(table_name)
		return self.run_cmd(select_cmd)

	def insert(self, entries, table_name=None):
		"""Inserts entries into table

		Args:
			entries: a dictionary where the keys are the table's columns and the
				values are the values intended to be inserted into the table
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			
		Returns:
			A boolean indiciating whether the insert succeeded. True if the
			insert works.
			

		Examples:
			>>> s = Sqlite3Table({'num':'INT'})
			>>> s.insert({'num':'1'})
			True
			>>> s.insert({'nonexhisting_column':'1'})
			False

		"""
		if not table_name:
			table_name = self.default_table
		insert_str = 'INSERT INTO "{0}" ({1}) VALUES ({2})'
		keys = [key for key in entries]
		rows = ','.join('"'+key+'"' for key in keys)
		vals = ','.join('"'+entries[key]+'"' for key in keys)
		insert_cmd = insert_str.format(table_name, rows, vals)
		try:
			self.run_cmd(insert_cmd)
			return True
		except:
			print >> sys.stderr, (
				'Cannot insert entries ({0}) '
				'into "{1}" table'
			).format(entries, table_name)
			return False

	def sudobulk_insert(self, list_entries, table_name=None, tolerance=0):
		"""A sudo-bulk insert function that simply repeatedly inserts values
		one at a time.

		Args:
			list_entries: a list of entries to be inserted
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			tolerance: the number (int) failed entries that will be tolerated 
				for the insert to be commited
			
		Returns:
			A boolean indiciating whether the sudobulk_insert succeeded. True if
			the	insert works.
			
		Examples:
			>>> s = Sqlite3Table({'num':'INT'})
			>>> list_entries = [{'num':str(i)} for i in xrange(10)]
			>>> s.sudobulk_insert(list_entries)
			True
			>>> list_entries.append({'num_bad':11})

		"""
		if not table_name:
			table_name = self.default_table
		failed = 0
		for entries in list_entries:
			try:
				self.insert(entries, table_name)
			except:
				failed += 1
				print(entries)
				#TODO: Propagete the errors
		if failed <= tolerance:
			self.db.commit()
			return True
		else:
			self.db.rollback()
			raise Exception('messed up')
			return False

	def queue_insert(self, data_queue, table_name=None, tolerance=0):
		"""Takes iter objects off a queue and sudobulk_inserts them into the
		table

		Args:
			data_queue: a queue filled with sudobulk_inserts `list_entries` used
				to be added to the 
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			tolerance: the number (int) failed entries that will be tolerated 
				for the sudo_bulkinsert to be commited
			
		Returns:
			Nothing

		Examples:
			>>> import Queue
			>>> q = Queue.Queue()
			>>> s = Sqlite3Table({'num':'INT'})
			>>> list_entries = [{'num':str(i)} for i in xrange(10)]
			>>> q.put(list_entries)
			>>> s.queue_insert(q)
			>>> len(s.select_all())
			10
		
		"""
		if not table_name:
			table_name = self.default_table
		while not data_queue.empty():
			self.sudobulk_insert(data_queue.get_nowait(),
				table_name=table_name, tolerance=tolerance)

	def delete(self, conditions, table_name=None):
		"""Deletes entries in table

		Args:
			conditions: a list of conditions (str) for the delete statement
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			
		Returns:
			Nothing

		Examples:
			>>> s = Sqlite3Table({'num':'INT', 'name':'TEXT'})
			>>> s.insert({'num':'1', 'name':'one'})
			True
			>>> s.insert({'num':'2', 'name':'one'})
			True
			>>> s.delete(['name = "one"','num = 1'])
			True
			>>> s.select_all()
			[{'num': 2, 'name': u'one'}]

		"""
		if not table_name:
			table_name = self.default_table
		delete_str = 'DELETE FROM "{0}" WHERE {1}'
		f_conditions = ' AND '.join(c for c in conditions)
		delete_cmd = delete_str.format(table_name, f_conditions)
		try:
			self.run_cmd(delete_cmd)
			return True
		except:
			print >> sys.stderr, (
				'Unable to delete from {0} given the conditions {1}'
			).format(conditions, table_name)
	
	def commit(self):
		"""Calls commit on the database. Not needed for `:memory:` db
		"""
		self.db.commit()

	def close(self, commit=False):
		"""Closes connection to database database
		"""
		if commit:
			self.commit()
		self.cur.close()
		self.db.close()

	def reconnect(self):
		"""Reconnects the db connection and cursor
		"""
		try:
			self.close()
		except:
			print >> sys.stderr, 'unable to close connect'
		finally:
			self.db = sqlite3.connect(self.db_name, check_same_thread=False)
			self.cur = self.db.cursor()

class RecentSqlite3table(Sqlite3Table):
	"""A Sqlite Database wrapper object that expires rows. It is a subclass of
	Sqlite3Table. 

	Args:
		clean_interval: the number of recent `sudobulk_insert` items (int) 
			that will be kept on the Sqlite database.
		clean_freq_interval: the frequency (int) in which terms of 
			`sudobulk_insert` that the service will check to clean records

	Attributes:
		clean_interval: see above for the args for `clean_interval`
		clean_freq_interval: see above for the args for `clean_freq_interval`
		rst_id: an internal counter that indicates the number of 
			sudobulk_inserts called on the object. The counter is appended to
			the rows of 

	"""

	def __init__(self, schema, db=None, table_name=None, clean_interval=100,
			clean_freq_interval=10):
		super(RecentSqlite3table, self).__init__(
			schema, db=db, table_name=table_name
		)
		self.clean_interval = clean_interval
		self.clean_freq_interval = clean_freq_interval
		self.rst_id = self.select_max()['MAX(RST_ID)']
		if not self.rst_id:
			self.rst_id = 0
		self.rst_id += 1

	def get_status():
		return self.rst_id

	def create_table(self, table_name, schema=None):
		"""Creates a table in the database with a RecentSqlite3table ID table

		Args:
			table_name: a str of the table name 
			schema: a dictionary schema used for the table
			
		Returns:
			A boolean indiciating if the table exists. Returns True if the table
			exhists; else returns False

		Examples:
			>>> s = RecentSqlite3table({'num':'INT', 'name':'TEXT'})
			>>> s.describe_table()
			u'(num INT, name TEXT, RST_ID INT)'
			>>> s = RecentSqlite3table({'C':'INT'}, \
				'/Users/kelvin.fann/Git/scripts/testx.db', 'default')
			>>> s.select_max()['MAX(RST_ID)']
			>>> s.sudobulk_insert([{'C':'2'}])
			True
			>>> s.sudobulk_insert([{'C':'2'}])
			True
			>>> s.select_max()['MAX(RST_ID)']
			2
			>>> s.close()
			>>> s = RecentSqlite3table({'C':'INT'}, \
				'/Users/kelvin.fann/Git/scripts/testx.db', 'default')
			>>> s.select_max()['MAX(RST_ID)']
			2
			>>> s.sudobulk_insert([{'C':'3'}])
			True
			>>> s.rst_id
			4
			>>> s.select_max()['MAX(RST_ID)']
			3
			>>> import os
			>>> os.remove('/Users/kelvin.fann/Git/scripts/testx.db')
		"""
		super(RecentSqlite3table, self).create_table(table_name, schema)
		alter_str = 'ALTER TABLE "{0}" ADD COLUMN {1}'
		alter_cmd = alter_str.format(table_name, 'RST_ID INT')
		return self.run_cmd(alter_cmd)

	def sudobulk_insert(self, list_entries, table_name=None, tolerance=0):
		"""A sudo-bulk insert function that simply repeatedly inserts values
		one at a time.

		Args:
			list_entries: a list of entries to be inserted
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			tolerance: the number (int) failed entries that will be tolerated 
				for the insert to be commited
			
		Returns:
			A boolean indiciating whether the sudobulk_insert succeeded. True if
			the	insert works.

		Examples:
			>>> schema = {'C': 'INTEGER'}
			>>> s = RecentSqlite3table(schema, clean_interval=3,\
					clean_freq_interval=2)
			>>> s.sudobulk_insert([{'C':'2'}])
			True
			>>> s.select_all()
			[{'C': 2, 'RST_ID': 1}]
			>>> s.sudobulk_insert([])
			True

		"""
		if not table_name:
			table_name = self.default_table
		failed = 0
		for entries in list_entries:
			try:
				self.insert(entries, table_name)
			except:
				failed += 1
				print entries
				#TODO: Propagete the errors
		if failed <= tolerance:
			self.db.commit()
			self.rst_id += 1
			if self.rst_id % self.clean_freq_interval == 0:
				self.clean()
			return True
		else:
			self.db.rollback()
			return False

	def insert(self, entries, table_name=None):
		"""A modified insert function that includes a `RST_ID` to the tables

		Args:
			entries: a dictionary where the keys are the table's columns and the
				values are the values intended to be inserted into the table
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			
		Returns:
			A boolean indiciating whether the insert succeeded. True if the
			insert works.

		Examples:
			>>> schema = {'bucket_start': 'INTEGER','bucket_end': 'INTEGER',\
			'orig_path': 'TEXT','count': 'INTEGER'}
			>>> s = RecentSqlite3table(schema, clean_interval=10)
			>>> s.insert({u'count': 2, u'orig_path': u'/dir/to/orig.txt',\
				u'bucket_end': 1439754480, u'bucket_start': 1439754460})
			[]
			>>> s.insert({'count': 2, 'orig_path': '/dir/to/orig.txt',\
				'bucket_end': 1439754480L, 'bucket_start': 1439754460L})
			[]
		"""
		if not table_name:
			table_name = self.default_table
		insert_str = 'INSERT INTO "{0}" ({1}) VALUES ({2})'
		keys = [key for key in entries]
		rows = ','.join('"{0}"'.format(key) for key in keys) + ',RST_ID'
		vals = (
			','.join('"{0}"'.format(entries[key]) for key in keys) +
			',{0}'.format(self.rst_id)
		)
		insert_cmd = insert_str.format(table_name, rows, vals)
		return self.run_cmd(insert_cmd)

	def select_max(self, table_name=None, column='RST_ID'):
		"""Gets the max value for a specified column

		Args:
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			column: the column in which to find the max from

		Returns:
			A dictionary with the {'MAX(column)': max_val}. max_val is None
			when no entries with the column exist

		Examples:
			>>> s = RecentSqlite3table({'num':'INT', 'name':'TEXT'})
			>>> s.select_max()
			{'MAX(RST_ID)': None}
			>>> s.sudobulk_insert([{'num': '1', 'name': 'hi'}])
			True
			>>> s.select_max()
			{'MAX(RST_ID)': 1}
			>>> s.sudobulk_insert([{'num': '2', 'name': 'hi'},\
				{'num': '2', 'name': 'hi'}])
			True
			>>> s.select_max()
			{'MAX(RST_ID)': 2}
			>>> s.select_max(column='num')
			{'MAX(num)': 2}

		"""
		if not table_name:
			table_name = self.default_table
		select_max_str = 'SELECT MAX({0}) FROM "{1}"'
		select_max_cmd = select_max_str.format(column, table_name)
		return self.run_cmd(select_max_cmd)[0]

	def clean(self, table_name=None, clean_interval=-1):
		"""Cleans values in database that have an RST_ID greater than the
		the current rst_id - specified interval 

		Args:
			table_name: a str of the table name, defaults to the attribute,
				`self.default_name`
			clean_interval: how many RST_ID's back the clean should keep. If 
				clean_interval == -1, clean utilizes the attribute 
				`self.clean_interval`

		Returns:
			A boolean indiciating if the clean succeeded. If the clean succeeded
			it returns True. 


		Examples:
			>>> s = RecentSqlite3table({'num':'INT', 'name':'TEXT'})
			>>> s.sudobulk_insert([{'num': '1', 'name': 'hi'}])
			True
			>>> s.clean(clean_interval=0)
			True
			>>> s.select_all()
			[]
			>>> s.select_max()
			{'MAX(RST_ID)': None}

		"""
		if not table_name:
			table_name = self.default_table
		if clean_interval == -1:
			clean_interval = self.clean_interval
		conditions = ['RST_ID < {0}'.format(self.rst_id - clean_interval)]
		return self.delete(conditions, table_name=table_name)

class Switch(object):
	"""A boolean object contatiner
	"""
	def __init__(self, boolean=False):
		self.value = boolean

	def switch(self):
		self.value = not self.value

##########################################
##Trunk###################################
##########################################

class RecentArray(object):
	"""A list that only keeps a certain number of recent records that will
	destructively overwrite expired records
	"""

	def __init__(self, limit=1000):
		self.recent_array = [None for _ in xrange(limit)]
		self.index_counter = 0

	def append(self, item):
		self.recent_array[self.index_counter] = item
		self.index_counter = (self.index_counter + 1) % len(self.recent_array)

	def get(self, index):
		return self.recent_array[index % len(self.recent_array)]	

	def get_onwards(self, index):
		if self.index_counter < index:
			return (
				self.recent_array[index:] +
				self.recent_array[:self.index_counter]
			)
		return self.recent_array[index:self.index_counter]

	def get_recent(self, num_items=-1):
		if num_items == -1 or num_items > len(self.recent_array):
			return self.get_recent(len(self.recent_array))
		if num_items >= self.index_counter:
			back = self.recent_array[:self.index_counter]
			front = self.recent_array[-(num_items-self.index_counter):]
		else:
			diff = self.index_counter - num_items
			back = self.recent_array[diff:diff+num_items]
			front = []
		return front + back

	def get_all(self):
		return self.recent_array

class DumpTable(object):
	"""A Table Dump

	"""

	def __init__(self, soft_limit=1000):
		self.rows = list()
		self.soft_limit = soft_limit

	def append(self, item):
		"""Appends a row entry to the DumpTable
		"""
		if type(item) is not dict:
			raise ValueError('Bad Param')
		elif len(self.rows) > self.soft_limit:
			raise Exception('DumpTable has exceeded soft limit')
		else:
			self.rows.append(item)

	def get_all(self):
		return self.rows

	def general_select(self, dict_params, comparison):
		"""A more comprehensive select that allows for individual comparison
		functions per paramter.

		Args:
			self: a DumpTable instance
			comparison: a function that takes in two paramters and produces
				a boolean return. Roughly follows the following format..

					def comparison(a,b):
						if some_func(a,b):
							return True
						elif:
							...
						else:
							return False

				Note that the `a` argument comes from the dict_params and 
				the `b` argument is the entry from the table. (I hope that
				makes sense)
			dict_params: a dictionary of paramters for the select 
				requirements. Follows the following format..
				{
					'parameter': (`arguments`*),
					'parameter2': (`arguments`*),
					...
				}
				Notice that the `arguments`* will be used as the `a` value
				in the comparison function
				***Notice*** For an example of a dict_params, please refer
				to the doctests of the DumpTable

		Returns:
			A list of row entries that satisify all the requirements from
			`dict_params` and the comparison function

		Examples:
			>>> dt = DumpTable()
			>>> gt = lambda a, b: a > b
			>>> dt.append({'name': 'ex1', 'count': 1})
			>>> dt.append({'name': 'ex2', 'count': 2})
			>>> dt.append({'name': 'ex3', 'count': 3}) 
			>>> dict_params = {'count': (2)}
			>>> tmp = dt.general_select(dict_params, gt)
			>>> len(tmp) == 1
			True
			>>> tmp[0] == {'name': 'ex1', 'count': 1}
			True

		"""
		matches = []
		for item in self.rows:
			match = True
			for key in dict_params:
				if (key not in item or 
						not comparison(dict_params[key], item[key])
					):
					match = False
					break
			if match:
				matches.append(item)
		return matches

	def c_general_select(self, c_dict_params):
		"""A more comprehensive select that allows for individual comparison
		functions per paramter.

		Args:
			self: a DumpTable instance
			c_dict_params: a dictionary of paramters for the select 
				requirements. Follows the following format..
				{
					'parameter': (comparison_func, (arguments*)),
					'parameter2': (comparison2_func, (arguments2*)),
					...
				}
				Notice that the comparison function is embeded inbedded into
				to the dictionary. `(arguments*)` will be used as the `a` value
				in the comparison function

		Returns:
			A list of row entries that satisify all the requirements from
			`c_dict_params`

		Examples:
			>>> dt = DumpTable()
			>>> gt = lambda a, b: a > b
			>>> dt.append({'name': 'ex1', 'count': 1})
			>>> dt.append({'name': 'ex2', 'count': 2})
			>>> dt.append({'name': 'ex3', 'count': 3})
			>>> c_dict_params = {'count': (gt, 2)}
			>>> tmp = dt.c_general_select(c_dict_params)
			>>> len(tmp)
			1
			>>> tmp[0] == {'name': 'ex1', 'count': 1}
			True
			>>> tmp = dt.c_general_select({})
			>>> len(tmp)
			0

		"""
		matches = []
		if len(c_dict_params) == 0:
			return matches
		for item in self.rows:
			match = True
			for key in c_dict_params:
				params = c_dict_params[key]
				if ((type(params) is not tuple and type(params) is not list)
					or len(params) == 0):
					raise ValueError('bad c_dict_params')
				comparison = TableComparison.parse_comparison(params[0])
				if key not in item or not comparison(params[1], item[key]):
					match = False
					break
			if match:
				matches.append(item)
		return matches

	def select(self, dict_params):
		comparison = TableComparison.eq
		return self.general_select(dict_params, comparison)

	def select_range(self, dict_params):
		comparison = TableComparison.range
		return self.general_select(dict_params, comparison)

	def select_gte(self, dict_params):
		comparison = TableComparison.gte
		return self.general_select(dict_params, comparison)

	def select_gt(self, dict_params):
		comparison = TableComparison.gt
		return self.general_select(dict_params, comparison)

	def select_lte(self, dict_params):
		comparison = TableComparison.lte
		return self.general_select(dict_params, comparison)

	def select_lt(self, dict_params):
		comparison = TableComparison.lt
		return self.general_select(dict_params, comparison)

class RecentTable(DumpTable):
	"""A list of dictionarys (a table) that only keeps a certain number of
	recent records. It will destructively overwrite expired rows
	"""

	def __init__(self, limit=1000):
		super(RecentTable, self).__init__()
		self.rows = [{} for _ in xrange(limit)]
		self.index_counter = 0

	def append(self, item):
		if type(item) is not dict:
			raise ValueError('Bad Param')
		else:
			self.recent_array[self.index_counter] = item
			self.index_counter = (self.index_counter+1) % len(self.recent_array)

class RecentArrayDumpTable(RecentArray):
	"""A list of DumpTable (list of dicts) that only keeps a certain number
	of recent tables. It will destructively overwrite expired tables
	"""

	def __init__(self, array_limit=1000, table_limit=100):
		super(RecentArrayDumpTable, self).__init__(limit=array_limit)
		self.table_limit = table_limit
		for i in xrange(array_limit):
			self.recent_array[i] = DumpTable(table_limit)

	def append(self, iter_item):
		self.recent_array[self.index_counter] = DumpTable(self.table_limit)
		for item in iter_item:
			self.recent_array[self.index_counter].append(item)
		self.index_counter = (self.index_counter + 1) % len(self.recent_array)


	def queue_append(self, data_queue):
		while not data_queue.empty():
			self.append(data_queue.get_nowait())

	def select(self, dict_params):
		comparison = TableComparison.eq
		return self.general_select(dict_params, comparison)

	def select_range(self, dict_params):
		comparison = TableComparison.range
		return self.general_select(dict_params, comparison)

	def general_select(self, dict_params, comparison):
		match = []
		for table in self.recent_array:
			match += table.general_select(dict_params, comparison)
		return match

	def c_general_select(self, c_dict_params):
		match = []
		for table in self.recent_array:
			match += table.c_general_select(c_dict_params)
		return match

	def get_all(self):
		return list(self._get_all())

	def _get_all(self):
		return (table.get_all() for table in self.recent_array)

	def get_onwards(self, index):
		return list(self._get_onwards(index))

	def _get_onwards(self, index):
		return (table.get_all() for table in 
			super(RecentArrayDumpTable, self).get_onwards(index))

	def get_recent(self, num_items=-1):
		return list(self._get_recent(num_items))

	def _get_recent(self, num_items=-1):
		return (table.get_all() for table in 
			super(RecentArrayDumpTable, self).get_recent(num_items))

class TableComparison(object):
	"""The comparison functions used for the tables
	"""

	@staticmethod
	def parse_comparison(func):
		if type(func) == str or type(func) == unicode:
			return TableComparison.str_to_func(func)
		elif hasattr(func, '__call__'):
			return func
		else:
			raise Exception('Bad Function, {0}'.format(func))
			return lambda a, b: False
	
	@staticmethod
	def str_to_func(func_str):
		func_dict = {
			'eq': TableComparison.eq,
			'range': TableComparison.range,
			'erange': TableComparison.erange,
			'gte': TableComparison.gte,
			'gt': TableComparison.gt,
			'lte': TableComparison.lte,
			'lt': TableComparison.lt,
		}
		if func_str[:7] == 'custom:':
			return lambda a, b: TableComparison.custom(func_str[7:], a, b) 
		elif func_str not in func_dict:
			raise Exception('Function ({0}) not supported'.format(func_str))
			return lambda a, b: False
		else:
			return func_dict[func_str]
	
	@staticmethod
	def eq(a, b):
		return a == b
	
	@staticmethod
	def range(a, b):
		if a[0] == None and a[1] == None:
			return True
		if a[0] == None:
			return a[1] >= b
		if a[1] == None:
			return a[0] <= b
		return a[0] <= b and a[1] >= b
	
	@staticmethod
	def erange(a, b):
		if a[0] == None:
			return a[1] > b
		if a[1] == None:
			return a[0] < b
		return a[0] < b and a[1] > b
	
	@staticmethod
	def gte(a, b):
		return a >= b
	
	@staticmethod
	def gt(a, b):
		return a > b
	
	@staticmethod
	def lte(a, b):
		return a <= b
	
	@staticmethod
	def lt(a, b):
		return a < b
	
	@staticmethod
	def custom(eval_statement, a, b):
		return eval(eval_statement)

if __name__ == "__main__":
	import doctest
	doctest.testmod()
