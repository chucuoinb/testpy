import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading

from db import Db
from cartmvc import Cartmvc

class ConfigInfo(Cartmvc,Db):
    notice = {}
    TYPE_TAX = 'tax'
    def __init__(self):
        self.notice['src'] = {
                'cart_type' : '',
                'cart_url' : '',
                'config' : {
                    'token' : '123456',
                    'version' : '',
                    'table_prefix' : '',
                    'charset' : '',
                    'image_category' : '',
                    'image_product' : '',
                    'image_manufacturer' : '',
                    'api' : {},
                    'folder' : '',
                    'file' : {},
                    'extend' : {},
                    }
                }

        self.notice['target'] =  {
                'cart_type' : '',
                'cart_url' : '',
                'config' : {
                    'token' : '123456',
                    'version' : '',
                    'table_prefix' : '',
                    'charset' : '',
                    'image_category' : '',
                    'image_product' : '',
                    'image_manufacturer' : '',
                    'api' : {},
                    'folder' : '',
                    'file' : {},
                    'extend' : {}
                    }
                }


        self.notice['setting'] = {'taxes' : 4,
            'manufacturers' : 4,
            'categories' : 4,
            'products' : 4,
            'customers' : 4,
            'orders' : 4}

        self.notice['is_migration_table'] = {'taxes' : 1,
            'manufacturers' : 1,
            'categories' : 1,
            'products' : 1,
            'customers' : 1,
            'orders' : 1}


    '''public function prepareDisplaySetupSource()
    {
            connector_url = 'http://localhost/opencart/cartmigration_connector/connector.copy.php?action=check&token=123456';
            check = self.getConnectorData(connector_url);
            //var_dump(check);
            check = self.getConnectorData(connector_url);

            data = check['data'];

            configKey = array('version', 'table_prefix', 'charset', 'image_product', 'image_category', 'image_manufacturer', 'extend');
            foreach(configKey as config_key){
                config_value = isset(data[config_key]) ? data[config_key] : '';
                _notice['src']['config'][config_key] = config_value;
                var_dump(_notice['src']['config'][config_key]);
            }
    }'''
    # get info frontent, ex : cart_type, url_cart, folder_image...
    
    def get_info_fronent(self):
        self.notice['src']['cart_type'] = 'oscomerce'
        self.notice['src']['cart_url'] = 'http://localhost/oscommerce/catalog/'

        self.notice['target']['cart_type'] = 'opencart'
        self.notice['target']['cart_url'] = 'http://localhost/opencart/'
        self.notice['is_migration_table'] = {'taxes' : 1,
            'manufacturers' : 1,
            'categories' : 1,
            'products' : 1,
            'customers' : 1,
            'orders' : 1}
        

        connector_url = self.notice['src']['cart_url'] + '/cartmigration_connector/connector.php?action=check&token=' + self.notice['src']['config']['token']
        check = self.getConnectorData(connector_url)
        data = check['data']
        configKey = ['version', 'table_prefix', 'charset', 'image_product', 'image_category', 'image_manufacturer', 'extend']
        for config_key in configKey:
        	self.notice['src']['config'][config_key] = data[config_key]


        connector_url = self.notice['target']['cart_url'] + '/cartmigration_connector/connector.php?action=check&token=' + self.notice['src']['config']['token']
        check = self.getConnectorData(connector_url)
        data = check['data']
        configKey = ['version', 'table_prefix', 'charset', 'image_product', 'image_category', 'image_manufacturer', 'extend']
        for config_key in configKey:
        	self.notice['target']['config'][config_key] = data[config_key]

        return self.notice
    
    #get id_src table
    def get_id_last(self,name_table):
        db = Db()
        conn = db.connectdb()
        #print(self.notice['src']['cart_url'])
        query = 'select id_src from table_migration where url_src = "' + self.notice['src']['cart_url'] + '" and url_desc = "' + self.notice['target']['cart_url'] + '" and name_table = "' + name_table + '"' 
        print(query) 
        result = db.selectdb(conn,query)
        return result

    




#pr = ConfigInfo()
#pr.get_info_fronent()
#print(notice)
#kq = pr.get_id_last()
#print(kq)