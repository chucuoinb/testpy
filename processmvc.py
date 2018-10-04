import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading
import sys
import os


from cartmvc import Cartmvc
from opencart import Opencart
from oscommerce import Oscommerce

class Process(Opencart,Oscommerce):
    def migrate(self,last_id,name_table,id_thread):
        last_id_f = last_id
        cart = Oscommerce()
        '''products = cart.getProductsMainExport()
        for x in range(0, 29):
            convert = cart.convertProductsExport(products,x)
            if convert:    
                self.productImport(convert)'''
        while 1:
            conn = mysql.connector.connect(user = 'root',password = '',host = 'localhost', database = 'carttest')
            mycursor=conn.cursor()
            query = """SELECT status from resume where id = %d""" %(id_thread)
            mycursor.execute(query)
            status=0
            if mycursor:
                result = mycursor.fetchall()
                status = result[0][0]
            if status == 2:
                break
            table_export = 'get' + name_table + 'sMainExport'
            var_table_export = getattr(cart, table_export)(last_id_f)
            if not var_table_export['data']:
                break
            for x in range(0, len(var_table_export['data'])):
                table_convert_export = 'convert' + name_table + 'Export'
                convert = getattr(cart, table_convert_export)(var_table_export,x)
                if convert:
                    table_import = name_table + 'Import'   
                    opencart_ob = Opencart() 
                    getattr(opencart_ob, table_import)(convert)
                    #print(x+1)
                    last_id_f = last_id_f +  1
                    #rint(last_id_f)
                    cnx = mysql.connector.connect(user='root',password = '', database='carttest')
                    cursor = cnx.cursor()
                    sql = """update table_migration set id_src = %d where id_client = %d and name_table = '%s'""" %(last_id_f,id_thread,name_table)
                    try:
                        cursor.execute(sql)
                        cnx.commit()
                    except:
                        cnx.rollback()

                    '''cursor = cnx.cursor()
                    sql = """insert into log(url_src,url_tar,id_src,type) values('%s','%s','%s','%s')""" %('http://localhost/oscommerce/catalog/','http://localhost/opencart/',last_id_f,'customer')
                    try:
                        cursor.execute(sql)
                        cnx.commit()
                    except:
                        cnx.rollback()'''

                else:
                    break

class myThread (threading.Thread,Process):
    def __init__(self, threadID, name, counter,last_id,id_thread,name_table):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.last_id = last_id
        self.id_thread = id_thread
        self.name_table = name_table
    def run(self):
        print ("Starting " + self.name)
        self.migrate(self.last_id,self.name_table,self.id_thread)
        print ("Exiting " + self.name)



def run(a,id_thread,name_table):
    id=id_thread
    conn = mysql.connector.connect(user = 'root',password = '',host = 'localhost', database = 'carttest')
    mycursor=conn.cursor()
    query = """SELECT id_src from table_migration where id_client = 2 and name_table = '%s'""" %(name_table)
    mycursor.execute(query)
    if mycursor:
        result = mycursor.fetchall()
        last_id = result[0][0]
    else:
        last_id = 0
    for i in range(1, a+1):
        thread_name = 'thread' + str(i)
        thread_name = myThread(i, thread_name, 1,last_id,id,name_table)
        thread_name.start()
        thread_name.join()

table_arr = ['Product','Customer']
#id = int(sys.argv[1])   
#update process id
processid = os.getpid()
cnx = mysql.connector.connect(user='root',password = '', database='carttest')
cursor = cnx.cursor()
sql = """update resume set id_process = %d where id = 2""" %(processid)
try:
    cursor.execute(sql)
    cnx.commit()
except:
    cnx.rollback()

for name_tb in table_arr:
    run(1,2,name_tb)


conn = mysql.connector.connect(user = 'root',password = '',host = 'localhost', database = 'carttest')
mycursor=conn.cursor()
query = """SELECT status from resume where id = 2"""
mycursor.execute(query)
status=0
if mycursor:
    result = mycursor.fetchall()
    status = result[0][0]
if status == 1:
    cnx = mysql.connector.connect(user='root',password = '', database='carttest')
    cursor = cnx.cursor()
    sql = """update resume set status = 3 where id = 2"""
    try:
        cursor.execute(sql)
        cnx.commit()
    except:
        cnx.rollback()


