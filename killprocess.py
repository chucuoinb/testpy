'''import mysql.connector

conn = mysql.connector.connect(user = 'root',password = '',host = 'localhost', database = 'kqxs')
mycursor=conn.cursor()
query = ("SELECT * from 3ngay1nam where so1 = 2")
mycursor.execute(query)
result = mycursor.fetchall()
print(result)
print(result[0][1])'''

import os
import requests
import urllib.parse
import mysql.connector
import sys

id = int(sys.argv[1])
idprocess  = int(sys.argv[2])
os.kill(idprocess,9)
