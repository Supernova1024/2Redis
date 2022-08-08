#!/usr/bin/python
## SQL to Redis

# import Redis and MySQL drivers
import redis
import MySQLdb
from collections import Counter
import time
from datetime import datetime
# create class
class dataProcessor(object):

	# Mysql server data
	MYSQL_IP_ADDRESS_SERVER = 'localhost'
	MYSQL_USER = 'root'
	MYSQL_PASSWORD = ''
	MYSQL_DATABASE_NAME = 'db_for_redis'

	# Redis server data
	REDIS_SERVER = 'localhost'

	# function to get data from mysql and to transfer it to redis
	@staticmethod
	def caculate_time_difference(start_milliseconds, end_milliseconds):
		diff_milliseconds = int(end_milliseconds) - int(start_milliseconds)
		seconds=(diff_milliseconds / 1000) % 60
		print('The difference is approx. %s second' % seconds)

	def sql_to_redis():
		r_redis = redis.StrictRedis(dataProcessor.REDIS_SERVER)
		print( "")
		print ("Connected to Redis successfully!")

		database = MySQLdb.connect(dataProcessor.MYSQL_IP_ADDRESS_SERVER, dataProcessor.MYSQL_USER, dataProcessor.MYSQL_PASSWORD, dataProcessor.MYSQL_DATABASE_NAME)
		print ("Connected to MySQL successfully!")
		print ("")

		cursor = database.cursor()
		# select = 'SELECT * FROM customers WHERE id = 9 LIMIT 100'
		select = "SELECT * FROM customers LIMIT 10 OFFSET 15";
		# select = 'SELECT * FROM records WHERE location_id = 9'
		cursor.execute(select)
		data = cursor.fetchall()

		# Clean redis before run again
		# This is for test purpose
		r_redis.delete("all_records")

		# Put all data from MySQL to Redis
		count = 0
		for row in data:
			pdt_dict = dict(name = row[1], address = row[2], iban = row[3], cc_number = row[4], phone = row[5])
			pipe.set(str(key), pdt_dict[key]).incr('auto_number').execute()
    		pipe.execute()
			print("=============count=======", count)
			count += 1

		# Close connection to DB and Cursor
		cursor.close()
		database.close()
	
	@staticmethod
	def get_data_from_redis():
		start_milliseconds = str(int(round(time.time() * 1000)))
		r2_redis = redis.StrictRedis(dataProcessor.REDIS_SERVER)

		list = []
		list = r2_redis.lrange("all_records", 0, 100)

		print (list)
		print ("")

		print ("Size of list:", len(list))
		print ("")

		word_list = [word for line in list for word in line.split()]
		print (word_list)
		print ("")
		end_milliseconds = str(int(round(time.time() * 1000)))
		dataProcessor.caculate_time_difference(start_milliseconds, end_milliseconds)
		words_to_count = (word for word in word_list if word[:1].isupper())
		top_ten = Counter(words_to_count)

		print ("Top 10 Most popular words:")
		print (top_ten.most_common(10), "\n")
if __name__ == '__main__':
	dataProcessor.sql_to_redis()
	dataProcessor.get_data_from_redis()