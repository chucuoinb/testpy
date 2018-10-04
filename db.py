import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading


class Db:
    user = 'root'
    password = ''
    host = 'localhost'
    database = 'carttest'

    def connectdb(self):
        conn = mysql.connector.connect(user = self.user,password = self.password,host = self.host, database = self.database)
        return conn

    def close_connect(self,conn):
        conn.close()
    
    def selectdb(self,conn,query):
        mycursor=conn.cursor()
        mycursor.execute(query)
        '''columns = [column[0] for column in mycursor.description]
        results = []
        if mycursor:
            for row in mycursor.fetchall():
                results.append(dict(zip(columns, row)))
            #result = mycursor.fetchall()
            return result[0]
        else:
            return False'''
        result = mycursor.fetchone()
        if result:
            result_id_desc = {}
            result_id_desc['id_desc'] = result[4]
            return result_id_desc
        else:
            return False


    def insertdb(self,conn,query):
        mycursor=conn.cursor()
        mycursor.execute(query)
        try:
            mycursor.execute(query)
            conn.commit()
            return mycursor.lastrowid
        except:
            conn.rollback()
            return False

    def updatedb(self,conn,query):
        mycursor=conn.cursor()
        mycursor.execute(query)
        try:
            mycursor.execute(query)
            conn.commit()
        except:
            conn.rollback()

     

    def deletedb(self,conn,query):
        mycursor=conn.cursor()
        mycursor.execute(query)
        try:
            mycursor.execute(query)
            conn.commit()
        except:
            conn.rollback()

    def arrayToInCondition(self,array):
        if not array:
            return "('None')"
        array = map({self, 'escape'}, array)
        result = ','.join(array)
        result = '(' + result + ')'
        return result


    def arrayToInsertCondition(self,array, allow_keys = None):
        if(array == False):
            return False

        keys = array.keys()
        data_key = data_value = []
        if(allow_keys == False):
            data_key = keys
            data_value = array.values()
            #for key,value in data_value.items():
                #data_value[key] = self.escape(value)

        else:
            for key in keys.items():
                if(key in allow_keys):
                    data_key.append(key)
                    value = array[key]
                    if(isinstance(value, int)):
                        data_value.append(value)
                    #else:
                        #data_value[] = self.escape(value)
                    
                
            
        
        if(data_key == False):
            return False
        
        key_condition = '(`' +  str.join('`, `', data_key) + '`)'
        value_condition = "(" + str.join(", ", data_value) + ")"
        condition = key_condition + " VALUES " + value_condition
        return condition







db = Db()
connect = db.connectdb()
query = """insert into log(url_src,url_tar,id_src,type) values('%s','%s','%s','%s')""" %('http://localhost/oscommerce/catalog/','http://localhost/opencart/',10,'Customer')
db.insertdb(connect,query)







