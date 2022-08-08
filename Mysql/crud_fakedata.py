import mysql.connector
import random
import time
from datetime import datetime

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="manage_excel_python"
)

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
	minutes=(diff_milliseconds/(1000*60))%60
	hours=(diff_milliseconds/(1000*60*60))%24
	print('The difference is approx ', hours,":",minutes,":",seconds)

def fake_record_generator(name, address, iban, cc_number, phone):
	milliseconds = str(int(round(time.time() * 1000)))
	n = str(random.random())
	random_str = milliseconds + n
	"""A fake record generator"""
	record = {
	'name': name + random_str,
	'address': address + random_str,
	'iban': iban + random_str,
	'cc_number': cc_number + random_str,
	'phone': phone + random_str
	}
	return record

def insert_db(limit):
	for x in range(0, limit):
		val_obj = fake_record_generator("name", "address", "iban", "cc_number", "phone")
		mycursor = mydb.cursor()
		sql = "INSERT INTO customers (name, address, iban, cc_number, phone) VALUES (%s, %s, %s, %s, %s)"
		val = (val_obj['name'], val_obj['address'], val_obj['iban'], val_obj['cc_number'], val_obj['phone'])
		mycursor.execute(sql, val)
		if x % 100000 == 0:
			print(x, "record inserted.")
	mydb.commit()

def select_item():
	start_milliseconds = str(int(round(time.time() * 1000)))
	print("start At : ", getHora())
	sql = "SELECT * FROM customers LIMIT 10000000 OFFSET 1";
	mycursor = mydb.cursor()
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	# for x in myresult:
	# 	print(x)
	print ("+++++++", len(myresult))
	print("End At : ", getHora())
	end_milliseconds = str(int(round(time.time() * 1000)))
	caculate_time_difference(start_milliseconds, end_milliseconds)

if __name__ == '__main__':
	# insert_db(10000000)
	select_item()