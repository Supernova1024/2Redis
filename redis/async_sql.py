#!/usr/bin/python
## SQL to Redis

# import Redis and MySQL drivers
import redis, json
import MySQLdb
from collections import Counter
import time
from datetime import datetime
import base64
import asyncio
import aiomysql
import pymysql.cursors

def getHora():
    data = datetime.now()
    hora = data.hour
    minu = data.minute
    seg = data.second
    mseg = data.microsecond
    str_hora = str(hora) + ':' + str(minu) + ':' + str(seg) + ':' + str(mseg)
    return str_hora

async def test(tablen, r_redis, sql):
	conn = await aiomysql.connect(
		host='localhost', user='root', password='', db='db_for_redis')
	try:
		print("startting " + tablen + " At " + str(getHora()))
		
		cur = await conn.cursor()
		print(tablen, await cur.execute(sql))
		await cur.execute(sql)
		data = await cur.fetchall()
		await cur.close()
		count = 0
		pdt_dict = dict()
		pdt_dict_str = ''
		for row in data:
			pdt_dict = dict(name = row[1], address = row[2], iban = row[3], cc_number = row[4], phone = row[5])
			pdt_dict_str = str(json.dumps(pdt_dict))
			r_redis.rpush(tablen, pdt_dict_str)
			if count % 100 == 0:
				print("=============count=======", count)
			count += 1

	finally:
		conn.close()
		print("setting " + tablen + " At " + str(getHora()))

async def compete(select_array, r_redis):
	await asyncio.gather(
		test('tablen1', r_redis, select_array[0]),
		test('tablen2', r_redis, select_array[1]),
		test('tablen3', r_redis, select_array[2]),
		test('tablen4', r_redis, select_array[3]),
		test('tablen5', r_redis, select_array[4]),
		test('tablen6', r_redis, select_array[5]),
		test('tablen7', r_redis, select_array[6]),
		test('tablen8', r_redis, select_array[7]),
		test('tablen9', r_redis, select_array[8]),
		test('tablen10', r_redis, select_array[9])
	)
	return 1

def caculate_time_difference(start_milliseconds, end_milliseconds):
	diff_milliseconds = int(end_milliseconds) - int(start_milliseconds)
	seconds=(diff_milliseconds / 1000) % 60
	print('The difference is approx. %s second' % seconds)

def sql_to_redis():
	r_redis = redis.StrictRedis('localhost')
	print( "")
	print ("Connected to Redis successfully!")
	for x in range(1, 11):
		r_redis.delete("tablen" + str(x))
	select_array = []
	for x in range(1, 11):
		select = "SELECT * FROM customers LIMIT " + str(100) + " OFFSET " + str(x);
		select_array.append(select)
	data = asyncio.run(compete(select_array, r_redis))
	return 1
	
def get_data_from_redis():
	start_milliseconds = str(int(round(time.time() * 1000)))
	r2_redis = redis.StrictRedis('localhost')
	list = []
	list = r2_redis.lrange("tablen1", 0, 1000000)
	# print (list)
	print ("")
	print ("Size of list:", len(list))
	print ("")
	word_list = []
	for line in list:
		word = json.loads(line)
		word_list.append(word)
	# print (list)
	print ("-------------", len(word_list))

	end_milliseconds = str(int(round(time.time() * 1000)))
	caculate_time_difference(start_milliseconds, end_milliseconds)

	# words_to_count = (word for word in word_list if word[:1].isupper())
	# top_ten = Counter(words_to_count)

	# print ("Top 10 Most popular words:")
	# print (top_ten.most_common(10), "\n")

if __name__ == '__main__':
	set_result = sql_to_redis()
	if set_result == 1:
		get_data_from_redis()
	# get_data_from_redis()