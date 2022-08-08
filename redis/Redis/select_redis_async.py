#===========================================================
#
# Unlock the comment of line 52, 77 to display sql result.
# Displaying the data will be taken long time
# 
#===========================================================
import redis, json
import MySQLdb
from collections import Counter
import time
from datetime import datetime
import base64
import asyncio
import aiomysql
import pymysql.cursors
import pandas as pd
global list_array
import aioredis

# count_of_data = 1000000
count_of_data = 100000

def getHora():
    data = datetime.now()
    hora = data.hour
    minu = data.minute
    seg = data.second
    mseg = data.microsecond
    str_hora = str(hora) + ':' + str(minu) + ':' + str(seg) + ':' + str(mseg)
    return str_hora

def caculate_time_difference(start_milliseconds, end_milliseconds):
	diff_milliseconds = int(end_milliseconds) - int(start_milliseconds)
	seconds=(diff_milliseconds / 1000) % 60
	print('The difference is approx. %s second' % seconds)

#===================== async selectting data from  redis  ====================
async def async_from_redis(tablename_k, list_array):
	# r2_redis = redis.StrictRedis('localhost')
	r2_redis = await aioredis.create_redis_pool('redis://localhost', encoding='utf8')
	print ("Connected to Redis successfully!")
	try:
		print(tablename_k + " has started!")
		list = []
		list_json_array = []
		# list =  r2_redis.lrange(tablename_k, 0, count_of_data)
		list = await r2_redis.lrange(tablename_k, 0, count_of_data)
		
	finally:
		print("Data of "+ str(len(list)) + " have been got from " + tablename_k + " At " + str(getHora()))
		for x in list:
			list_array.append(json.loads(x))
		dataset = pd.DataFrame(list_array)
		OutputDataSet = dataset
		print(OutputDataSet)
async def async_from_redis_array(list_array):
	await asyncio.gather(
		async_from_redis('tablename_k1', list_array),
		async_from_redis('tablename_k2', list_array),
		async_from_redis('tablename_k3', list_array),
		async_from_redis('tablename_k4', list_array),
		async_from_redis('tablename_k5', list_array),
		async_from_redis('tablename_k6', list_array),
		async_from_redis('tablename_k7', list_array),
		async_from_redis('tablename_k8', list_array),
		async_from_redis('tablename_k9', list_array),
		async_from_redis('tablename_k10', list_array)
	)
	return 1

def get_data_from_redis():
	list_array = []
	
	
	start_milliseconds = str(int(round(time.time() * 1000)))
	print("Start At : " + str(getHora()))
	asyncio.run(async_from_redis_array(list_array))
	print("End At : " + str(getHora()))

	end_milliseconds = str(int(round(time.time() * 1000)))
	caculate_time_difference(start_milliseconds, end_milliseconds)

if __name__ == '__main__':
	get_data_from_redis()