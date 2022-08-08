#======================================================================
#
#	Unlock the comment of line 52, 77 to test the total code
#		- But It will delete all data of Redis and reset. 
#		- It will be taken a long time(about 30minute).
#	So use only B in the main function after using A once.
#		- I recommend you using the select_redis_async.py.
#		- It only selects the data from Redis without resetting.
#
#======================================================================
import redis, json
import MySQLdb
from collections import Counter
import time
from datetime import datetime
import base64
import asyncio
import aiomysql
import pymysql.cursors

global list_array

count_of_data = 1000000
# count_of_data = 10

def getHora():
	data = datetime.now()
	hora = data.hour
	minu = data.minute
	seg = data.second
	mseg = data.microsecond
	str_hora = str(hora) + ':' + str(minu) + ':' + str(seg) + ':' + str(mseg)
	return str_hora

def caculate_time_difference(kins_of_time, start_milliseconds, end_milliseconds):
	diff_milliseconds = int(end_milliseconds) - int(start_milliseconds)
	seconds=(diff_milliseconds / 1000) % 60
	minutes=(diff_milliseconds/(1000*60))%60
	hours=(diff_milliseconds/(1000*60*60))%24
	print(kins_of_time,'--The difference is approx ', hours,":",minutes,":",seconds)


#===================== async selectting data from  Mysql  &   async setting to redis===========
async def async_sql_to_redis(tablename_k, r_redis, sql):
	sql_connection = await aiomysql.connect(
		host='localhost', user='root', password='', db='db_for_redis')
	try:
		print (tablename_k, "Connected to Mysql successfully!")							 
		print(tablename_k, "selectting and setting data have been started At " + str(getHora()))
		sql_cursor = await sql_connection.cursor()
		await sql_cursor.execute(sql)
		data = await sql_cursor.fetchall()
		await sql_cursor.close()
		count = 0
		pdt_dict = dict()
		pdt_dict_str = ''
		for row in data:
			pdt_dict = dict(name = row[1], address = row[2], iban = row[3], cc_number = row[4], phone = row[5])
			pdt_dict_str = str(json.dumps(pdt_dict))
			# r_redis.rpush(tablename_k, pdt_dict_str)
	finally:
		sql_connection.close()
		print("data of " + str(len(data)) + " have been set to " + tablename_k + " of redis At " + str(getHora()))

async def async_sql_to_redis_array(select_array, r_redis):
	await asyncio.gather(
		async_sql_to_redis('tablename_k1', r_redis, select_array[0]),
		async_sql_to_redis('tablename_k2', r_redis, select_array[1]),
		async_sql_to_redis('tablename_k3', r_redis, select_array[2]),
		async_sql_to_redis('tablename_k4', r_redis, select_array[3]),
		async_sql_to_redis('tablename_k5', r_redis, select_array[4]),
		async_sql_to_redis('tablename_k6', r_redis, select_array[5]),
		async_sql_to_redis('tablename_k7', r_redis, select_array[6]),
		async_sql_to_redis('tablename_k8', r_redis, select_array[7]),
		async_sql_to_redis('tablename_k9', r_redis, select_array[8]),
		async_sql_to_redis('tablename_k10', r_redis, select_array[9])
	)
	return 1

def sql_to_redis():
	r_redis = redis.StrictRedis('localhost')
	print( "")
	print ("Connected successfully to Redis for setting !")
	for x in range(1, 11):
		# r_redis.delete("tablename_k" + str(x))
	select_array = []
	for x in range(1, 11):
		select = "SELECT * FROM customers LIMIT " + str(count_of_data) + " OFFSET " + str(x);
		select_array.append(select)
	data = asyncio.run(async_sql_to_redis_array(select_array, r_redis))
	return 1


#===================== async selectting data from  redis  ====================
async def async_from_redis(tablename_k):
	r2_redis = redis.StrictRedis('localhost')
	print ("Connected successfully to Redis for selectting !")
	try:
		list = r2_redis.lrange(tablename_k, 0, count_of_data)
	finally:
		print("Data of "+ str(len(list)) + " have been got from " + tablename_k + " At " + str(getHora()))
async def async_from_redis_array():
	await asyncio.gather(
		async_from_redis('tablename_k1'),
		async_from_redis('tablename_k2'),
		async_from_redis('tablename_k3'),
		async_from_redis('tablename_k4'),
		async_from_redis('tablename_k5'),
		async_from_redis('tablename_k6'),
		async_from_redis('tablename_k7'),
		async_from_redis('tablename_k8'),
		async_from_redis('tablename_k9'),
		async_from_redis('tablename_k10'),
	)
	return 1

def get_data_from_redis():
	list_array = []
	start_milliseconds = str(int(round(time.time() * 1000)))
	print("Start At : ", getHora())
	data = asyncio.run(async_from_redis_array())
	print("End At : ", getHora())
	end_milliseconds = str(int(round(time.time() * 1000)))
	caculate_time_difference("Getting data from redis", start_milliseconds, end_milliseconds)

if __name__ == '__main__':

	##=============================A==================================
	# start_milliseconds_total = str(int(round(time.time() * 1000)))
	# set_result = sql_to_redis()
	# if set_result == 1:
	# 	get_data_from_redis()
	# end_milliseconds_total = str(int(round(time.time() * 1000)))
	# caculate_time_difference("Total", start_milliseconds_total, end_milliseconds_total)

	##=============================B==================================
	get_data_from_redis()