import re
import sys
import pickle
import sqlparse
import numpy as np

# complete database of .csv files as dictionary 
database = {}

def get_schema():
	'''
		database is map of tables where each table is a map of its attributes, 
		each attribute maps to list of values.
	'''
	database = {}
	with open('./metadata.txt', 'r') as f:
		table_state = 0
		table_name = ''
		table_attrs = ''
		for line in f:
			if '<begin_table>' in line:
				table = 1
			elif '<end_table>' in line:
				table = 0
			else:
				if table == 1:
					table_name = line.replace('\r\n', '')
					database[table_name] = {}
					database[table_name]['records'] = []
					database[table_name]['schema'] = []
					table = 2
				else:
					table_attrs = line.replace('\r\n', '')
					database[table_name]['schema'].append(table_attrs)
	return database

def fill_database(database):
	# fill the database with given range of values
	for table in database.keys():
		with open('./'+str(table)+'.csv') as f:
			for line in f:
				values = [float(value) for value in line.split(',') if not value == '']
				database[table]['records'].append(tuple(values))
	return database
	
def debug(database):
	# to test/debug	
	for table in database.keys():
		print table
		for records in database[table]['records']:
			print records
	
def init_table():
	# hardcopy of default table
	table = {}
	table['schema'] = []
	table['records'] = []
	return table

def get_table(tables, attributes):
	# gives presence of an attribute in tables
	presence = {}		
	for table in tables:
		try:
			for attr in database[table]['schema']:		
				try:
					presence[str(attr)].append(str(table))
				except KeyError:
					presence[str(attr)] = [str(table)]
				try:
					presence[str(table)+'.'+str(attr)].append(str(table))
				except KeyError:
					presence[str(table)+'.'+str(attr)] = [str(table)]
		except KeyError:
			print str(table)+' not found in database'
			sys.exit()
	return presence

def parse_query(query):
	# parses the query & gets attributes, table
	print query
	tables = []
	iterator = 0
	keywords = []
	attributes = []
	conditional = None
	query = query.replace(';', '')
	statement = sqlparse.parse(query)[0]
	for token in statement.tokens:

		if str(token.ttype) == 'Token.Keyword.DML' or str(token.ttype) == 'Token.Keyword':
			# keywords
			keywords.append(str(token))
		
		if iterator == 0 and (str(token.ttype) == 'None' or str(token.ttype) == 'Token.Wildcard'):
			# attributes
			string_attributes = re.sub(' +', ' ', str(token.value))
			attributes = string_attributes.replace(' ', '').split(',')
			iterator += 1
			
		elif iterator == 1 and str(token.ttype) == 'None':
			# tables
			string_tables = re.sub(' +', ' ', str(token.value))
			tables = string_tables.replace(' ', '').split(',')
			iterator += 1
			
		elif iterator == 2 and str(token.ttype) == 'None':
			# conditional
			conditional = str(token).replace('where' , '')
			iterator += 1

	if attributes == [] or tables == []:
		# error handling
		print 'Either attributes or tables are missing in query'

	return attributes, tables, keywords, conditional

def cross_tables(tables, current_table, result):
	# join all tables in the query
	if len(tables) == current_table:
		# base condition
		return

	table = tables[current_table]
	result['schema'].extend([str(table)+'.'+str(attr) for attr in database[table]['schema'] if not str(table)+'.' in attr])
			
	if len(result['records']) == 0:
		result['records'] = database[tables[current_table]]['records']
	else:
		temp = []
		for record in database[tables[current_table]]['records']:
			temp.extend([each_record+record for each_record in result['records']])
		result['records'] = temp
	cross_tables(tables, current_table+1, result)
	
	return result

def conditional_table(table, query, presence):
	# no conditionals
	if query == None:
		return table

	operators = ['=', '>', '<']
	conditionals = query.replace('(', '').replace(')', '').replace('and', '^').replace('or', '^')
	conditionals = conditionals.split('^')

	i = 0
	records = table['records']
	bitmap = np.zeros((len(records), ), dtype=float)
	
	for record in records:
		temp_query = query
		for conditional in conditionals:
			found_operator = None
			conditional = conditional.strip()
			for operator in operators:
				if operator in conditional:
					found_operator = operator
					[attribute, value] = conditional.split(operator)
					value = value.strip()
					attribute = attribute.strip()
					break
			
			if found_operator == None:
				print 'operations defined are', operators
				sys.exit()
			else:
				try:
					# Error Handler
					if len(presence[attribute]) > 1:
						print str(attribute)+' is ambiguous'
						sys.exit()

					# get index of conditional variable
					if '.' in attribute:
						index = table['schema'].index(attribute)
					else:
						index = table['schema'].index(str(presence[attribute][0])+'.'+str(attribute))

				except KeyError:
					print str(attribute)+' attribute not found in any table'
					sys.exit()

				
				if value in presence.keys():
					# Error Handling
					if len(presence[value]) > 1:
						print str(value)+' is ambiguous'
						sys.exit()

					# get index of joining attribute
					if '.' in value:
						join_attribute = value
					else:
						join_attribute = str(presence[value][0])+'.'+str(value)
					join_idx = table['schema'].index(join_attribute)

					# mark true records
					if found_operator == '=':
						temp_query = temp_query.replace(conditional, str(int(record[index] == record[join_idx])))
					elif found_operator == '<':
						temp_query = temp_query.replace(conditional, str(int(record[index] < record[join_idx])))
					elif found_operator == '>':
						temp_query = temp_query.replace(conditional, str(int(record[index] > record[join_idx])))
				else:
					try:
						# mark true records
						if found_operator == '=':
							temp_query = temp_query.replace(conditional, str(int(records[i][index] == float(value))))
						if found_operator == '>':
							temp_query = temp_query.replace(conditional, str(int(records[i][index] > float(value))))
						if found_operator == '<':
							temp_query = temp_query.replace(conditional, str(int(records[i][index] < float(value))))
					except:
						print str(value)+' : Unknown variable in query.'
		try:
			bitmap[i] = eval(temp_query)
			i += 1
		except SyntaxError:
			print 'missing parenthesis'
			sys.exit()

	# select records based on bitmap
	records = table['records']
	conditional_table = init_table()
	conditional_table['schema'] = table['schema']
	for i in range(bitmap.shape[0]):
		if bitmap[i] >= 1:
			conditional_table['records'].append(records[i])

	return conditional_table

def project_table(table, attributes, keywords, presence):

	# project table on attributes
	for attr in attributes:
		temp = re.sub(' +', '', attr).replace(')', '')
		if 'max' in temp:
			# max element over a column
			temp = temp.replace('max(', '')
			try:
				# Error Handler
				if len(presence[str(temp)]) > 1:
					print str(temp)+' is ambiguous'
					sys.exit()
				if len(presence[str(temp)]) < 1:
					print str(temp)+' is not present in any database table'
					sys.exit()

				if '.' in temp:
					index = table['schema'].index(temp)
				else:
					index = table['schema'].index(str(presence[temp][0])+'.'+str(temp))
				
				print '< MAX('+str(temp)+') >'
				if not table['records'] == []:				
					ans = table['records'][0][index]
					for record in table['records']:
						ans = max(ans, record[index])
					print ans
			except KeyError:
				print str(temp)+' not found in table'
			sys.exit()

		elif 'min' in temp:
			# min element over a column
			temp = temp.replace('min(', '')
			try:
				# Error Handler
				if len(presence[str(temp)]) > 1:
					print str(temp)+' is ambiguous'
					sys.exit()
				if len(presence[str(temp)]) < 1:
					print str(temp)+' is not present in any database table'
					sys.exit()

				if '.' in temp:
					index = table['schema'].index(temp)
				else:
					index = table['schema'].index(str(presence[temp][0])+'.'+str(temp))
				print '< MIN('+str(temp)+') >'
				if not table['records'] == []:
					ans = table['records'][0][index]
					for record in table['records']:
						ans = min(ans, record[index])
					print ans
			except KeyError:
				print str(temp)+' not found in table'
			sys.exit()

		elif 'sum' in temp:
			# sum over a column
			# what about sum when crossed with other table
			temp = temp.replace('sum(', '')
			try:
				# Error Handler
				if len(presence[str(temp)]) > 1:
					print str(temp)+' is ambiguous'
					sys.exit()
				if len(presence[str(temp)]) < 1:
					print str(temp)+' is not present in any database table'
					sys.exit()

				ans = 0
				if '.' in temp:
					index = table['schema'].index(temp)
				else:
					index = table['schema'].index(str(presence[temp][0])+'.'+str(temp))
				print '< SUM('+str(temp)+') >'
				if not table['records'] == []:
					for record in table['records']:
						ans += record[index]
					print ans
			except KeyError:
				print str(temp)+' not found in table'
			sys.exit()

		elif 'avg' in temp:
			# average over a column
			# what about sum when crossed with other table
			temp = temp.replace('average(', '')
			try:
				# Error Handler
				if len(presence[str(temp)]) > 1:
					print str(temp)+' is ambiguous'
					sys.exit()
				if len(presence[str(temp)]) < 1:
					print str(temp)+' is not present in any database table'
					sys.exit()

				ans = 0
				if '.' in temp:
					index = table['schema'].index(temp)
				else:
					index = table['schema'].index(str(presence[temp][0])+'.'+str(temp))
				print '< AVG('+str(temp)+') >'
				if not table['records'] == []:
					for record in table['records']:
						ans += record[index]
					print (ans*1.0) / len(table['records'])
			except KeyError:
				print str(temp)+' not found in table'
			sys.exit()

	indices = []
	if not '*' in attributes:
		# # ignore joined attributes
		# project_attr = []
		# for record in table['records']:
		# 	unique_attr = set(record)
		# 	indices = [record.index(attr) for attr in unique_attr]
		# 	for i in indices:
		# 		if not table['schema'][i] in project_attr:
		# 			project_attr.append(table['schema'][i])

		# temp = []
		# indices = [table['schema'].index(attr) for attr in project_attr]
		# for record in table['records']:
		# 	temp.append(tuple([record[i] for i in indices]))

		# table['schema'] = project_attr
		# if 'distinct' in keywords:
		# 	table['records'] = set(temp)
		# else:
		# 	table['records'] = temp

		# Error Handler
		for attribute in attributes:		
			try:
				if len(presence[str(attribute)]) > 1:
					print str(attribute)+' is ambiguous'
					sys.exit()
				if len(presence[str(attribute)]) < 1:
					print str(attribute)+' is not present in any database table'
					sys.exit()
			except KeyError:
				print str(attribute)+' not present in database'
				sys.exit()

		for attr in attributes:
			try:	
				if '.' in attr:
					indices.append(table['schema'].index(attr))
				else:
					indices.append(table['schema'].index(str(presence[attr][0])+'.'+str(attr)))
			except ValueError:
				print str(attr)+' attribute not found in any table'
				sys.exit()

		temp = []
		for record in table['records']:
			temp.append(tuple([record[i] for i in indices]))

		table['schema'] = [attr for attr in attributes]

		# distinct records
		if 'distinct' in keywords:
			table['records'] = set(temp)
		else:
			table['records'] = temp

		# # ignore joined attributes
		# project_attr = []
		# for record in table['records']:
		# 	unique_vals = set(record)
		# 	indices = [record.index(val) for val in unique_vals]
		# 	for i in indices:
		# 		if not table['schema'][i] in project_attr:
		# 			project_attr.append(table['schema'][i])

		# temp = []
		# indices = [table['schema'].index(attr) for attr in project_attr]
		# for record in table['records']:
		# 	temp.append(tuple([record[i] for i in indices]))

		# table['schema'] = project_attr

	return table

def showtable(table):

	print '\noutput:'
	print ','.join([str(attribute) for attribute in table['schema']])
	for record in table['records']:
		print ','.join([str(value) for value in record])

if __name__ == '__main__':
	
	if not len(sys.argv) == 2:
		print 'proper usage: python query_processing.py "<query>"'		
		sys.exit()

	query = sys.argv[1]
	database = fill_database(get_schema())
	attributes, tables, keywords, conditionals = parse_query(query)
	attr_presence = get_table(tables, attributes)
	all_join_table = cross_tables(tables, 0, init_table())
	conditioned_table = conditional_table(all_join_table, conditionals, attr_presence)
	projected_table = project_table(conditioned_table, attributes, keywords, attr_presence)
	showtable(projected_table)