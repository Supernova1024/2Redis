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
	try:
		print(str(tablename_k) + " has started!")
		list1 = []
		list_json_array = []
		# Write processing here!!!
		# When processing is completed, set the flag of the processing number
		flag = tablename_k
	finally:
		# for x in flag:
		list_array.append(flag)
		print("OutputDataSet" + str(tablename_k) + "-------", list_array)
async def async_from_redis_array(list_array):
	await asyncio.gather(
		async_from_redis(1, list_array),
		async_from_redis(2, list_array),
		async_from_redis(3, list_array),
		async_from_redis(4, list_array),
		async_from_redis(5, list_array),
		async_from_redis(6, list_array),
		async_from_redis(7, list_array),
		async_from_redis(8, list_array),
		async_from_redis(9, list_array),
		async_from_redis(10, list_array)
	)
	return 1

def get_data_from_redis():
	list_array = []
	start_milliseconds = str(int(round(time.time() * 1000)))

	print("Start At : " + str(getHora()))
	data = asyncio.run(async_from_redis_array(list_array))
	print("@@@@@@@@@@@@@@@",list_array)
	print("End At : " + str(getHora()))

	end_milliseconds = str(int(round(time.time() * 1000)))
	caculate_time_difference(start_milliseconds, end_milliseconds)

if __name__ == '__main__':
	get_data_from_redis()