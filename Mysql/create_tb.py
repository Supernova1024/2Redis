import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="db_for_redis"
)
mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255), iban VARCHAR(255), cc_number VARCHAR(255), phone VARCHAR(255))")
print("---tb has created---")