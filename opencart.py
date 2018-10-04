import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading
import html
import Image
import re


from cartmvc import Cartmvc
from config import ConfigInfo

class Opencart(Cartmvc,ConfigInfo):

    def ConfigSource(self):

        response = self._defaultResponse()
        default_config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT cfg.*, lg.* FROM oc_setting AS cfg LEFT JOIN oc_language AS lg ON lg.code = cfg.value WHERE cfg.key = 'config_language'"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT cfg.*, cur.* FROM oc_setting AS cfg LEFT JOIN oc_currency AS cur ON cur.code = cfg.value WHERE cfg.key = 'config_currency'"}}})
                
            
        
        
        if(default_config == False or default_config['result'] != "success"):
            return self.errorConnector(False)
        
        default_config_data = default_config['data']
        if(default_config_data and default_config_data['languages'] and default_config_data['currencies']):
            self._notice['src']['language_default'] = default_config_data['languages']['0']['language_id'] if self.iset(default_config_data['languages']['0'],'language_id') else 1
            self._notice['src']['currency_default'] = default_config_data['currencies']['0']['currencie_id'] if self.iset(default_config_data['currencies']['0'],'currencie_id') else 1
        
       
        
        self._notice['src']['category_root'] = 1
        self._notice['src']['site'] = {1 : 'Default Shop'}
        
        self._notice['src']['categoryData'] = {1 : 'Default Category'}
        
        self._notice['src']['attributes'] = {1 : 'Default Attribute'}
        
        config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT * FROM language"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT * FROM currency"},
                
                'orders_status' : {'type' : 'select',
                    'query' : "SELECT * FROM order_status WHERE language_id = '" + self+_notice['src']['language_default'] + "'"}
                
                'customer_group_description' : {'type' : 'select',
                    'query' : "SELECT * FROM customer_group_description WHERE language_id = '" + self+_notice['src']['language_default'] + "'"}}})
                
                
            
        
        if(config == False or config['result'] != "success"):
            return self.errorConnector(False)
        
        
        config_data = config['data']
        language_data = currency_data = order_status_data = customer_group_data = {}
        for language_row in config_data['languages'].items():
            lang_id = language_row['language_id']
            lang_name = language_row['name'] + "(" + language_row['code'] + ")"
            language_data[lang_id] = lang_name
        
        for order_status_row in config_data['orders_status'].items():
            order_status_id = order_status_row['order_status_id']
            order_status_name = order_status_row['name']
            order_status_data[order_status_id] = order_status_name
        
        for currency_row in config_data['currencies'].items():
            currency_id = currency_row['currency_id']
            currency_name = currency_row['title']
            currency_data[currency_id] = currency_name
        
        for cus_status_row in config_data['customer_group_description'].items():
            cus_status_id = cus_status_row['customer_group_id']
            cus_status_name = cus_status_row['name']
            customer_group_data[cus_status_id] = cus_status_name
        
        self._notice['src']['languages'] = language_data
        self._notice['src']['order_status'] = order_status_data
        self._notice['src']['currencies'] = currency_data
        self._notice['src']['support']['country_map'] = False
        self._notice['src']['support']['customer_group_map'] = True
        self._notice['src']['customer_group'] = customer_group_data
        
        response['result'] = 'success'
        return response
    
    
    def ConfigTarget(self):

        response = self._defaultResponse()
        default_config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT cfg.*, lg.* FROM oc_setting AS cfg LEFT JOIN oc_language AS lg ON lg.code = cfg.value WHERE cfg.key = 'config_language'"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT cfg.*, cur.* FROM oc_setting AS cfg LEFT JOIN oc_currency AS cur ON cur.code = cfg.value WHERE cfg.key = 'config_currency'"}}})
                
            
        
        if(default_config == False or default_config['result'] != "success"):
            return self.errorConnector(False)
        
        default_config_data = default_config['data']
        if(default_config_data and default_config_data['languages'] and default_config_data['currencies']):
            self._notice['target']['language_default'] = default_config_data['languages']['0']['language_id'] if self.iset(default_config_data['languages']['0'],'language_id') else 1
            self._notice['target']['currency_default'] = default_config_data['currencies']['0']['currency_id'] if self.iset(default_config_data['currencies']['0'],'currency_id') else 1
        
        self._notice['target']['category_root'] = 1
        self._notice['target']['support']['site_map'] = False
        self._notice['target']['support']['category_map'] = False
        self._notice['target']['support']['attribute_map'] = False
        config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT * FROM language"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT * FROM currency"},
                
                'orders_status' : {'type' : 'select',
                    'query' : "SELECT * FROM order_status WHERE language_id = '" + self._notice['target']['language_default'] + "'"},
                
                'customer_group_description' : {'type' : 'select',
                    'query' : "SELECT * FROM customer_group_description WHERE language_id = '" + self._notice['target']['language_default'] + "'"}}})
                
            
        
        if(config == False or config['result'] != "success"):
            return self.errorConnector(False)
        
        config_data = config['data']
        language_data = order_status_data = currency_data = country_data = customer_group_data = {}
        for language_row in config_data['languages'].items():
            lang_id = language_row['language_id']
            lang_name = language_row['name'] + "(" + language_row['code'] + ")"
            language_data[lang_id] = lang_name
        
        for order_status_row in  config_data['orders_status'].items():
            order_status_id = order_status_row['order_status_id']
            order_status_name = order_status_row['name']
            order_status_data[order_status_id] = order_status_name
        
        for currency_row in config_data['currencies'].items():
            currency_id = currency_row['currency_id']
            currency_name = currency_row['title']
            currency_data[currency_id] = currency_name
        
        for cus_status_row in config_data['customer_group_description'].items():
            cus_status_id = cus_status_row['customer_group_id']
            cus_status_name = cus_status_row['name']
            customer_group_data[cus_status_id] = cus_status_name
        
        # read file config and return content
        
        file_url = self.getConnectorUrl('file')
        check_config_file = self.getConnectorData(file_url, {'files' : {{'type' : 'content',
                        'path' : 'config.php'}}})
                    
                
            

        match = re.search('/define\(\'DB_DATABASE\', \'(.+)\'\)/', check_config_file['data'][0])
        if(match):
             self._notice['target']['config']['db_name'] = str(match[1])
        
        
        self._notice['target']['support']['language_map'] = True
        self._notice['target']['languages'] = language_data
        self._notice['target']['support']['order_status_map'] = True
        self._notice['target']['order_status'] = order_status_data
        self._notice['target']['support']['currency_map'] = True
        self._notice['target']['currencies'] = currency_data
        self._notice['target']['support']['country_map'] = False
        self._notice['target']['countries'] = country_data
        self._notice['target']['support']['customer_group_map'] = True
        self._notice['target']['customer_group'] = customer_group_data
        self._notice['target']['support']['taxes'] = True
        self._notice['target']['support']['manufacturers'] = True
        self._notice['target']['support']['categories'] = True
        self._notice['target']['support']['products'] = True
        self._notice['target']['support']['customers'] = True
        self._notice['target']['support']['orders'] = True
        self._notice['target']['support']['reviews'] = True
        response['result'] = 'success'
        return response
    






    def getTaxesExtExport(self,taxes):
        taxIds = self.duplicateFieldValueFromList(taxes['data'], 'tax_class_id')
        tax_id_con = self.arrayToInCondition(taxIds)
        taxesExt = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'tax_rates' : {'type' : 'select',
                    'query' : "SELECT * FROM tax_rule AS tr LEFT JOIN tax_rate AS tx ON tr.tax_rate_id = tx.tax_rate_id WHERE tax_class_id IN " . tax_id_con}}})
        if(taxesExt == False or taxesExt['result'] != 'success'):
            return self.errorConnector()

        taxZoneIds = self.duplicateFieldValueFromList(taxesExt['data']['tax_rates'], 'geo_zone_id')
        tax_zone_query = self.arrayToInCondition(taxZoneIds)
        taxesExtRel = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'geo_zone' : {'type' : 'select',
                    'query' : "SELECT * FROM geo_zone WHERE geo_zone_id IN " + tax_zone_query},
                'zone_to_geo_zone' : {'type' : 'select',
                    'query' : "SELECT gz.*, ztgz.*, z.name as zone_name, z.code as zone_code, c.iso_code_2, c.name as country_name FROM geo_zone AS gz LEFT JOIN zone_to_geo_zone AS ztgz ON ztgz.geo_zone_id = gz.geo_zone_id LEFT JOIN zone AS z ON z.zone_id = ztgz.zone_id LEFT JOIN country AS c ON c.country_id = ztgz.country_id WHERE ztgz.geo_zone_id IN" + tax_zone_query}}})
        if(taxesExtRel ==False or taxesExtRel['result'] != 'success'):
            return self.errorConnector()

        taxesExt = self.syncConnectorObject(taxesExt, taxesExtRel)
        return taxesExt
    

    def convertTaxExport(self,tax, taxesExt):
        taxProduct = taxCustomer = taxZone = {}

        tax_product = self.constructTaxProduct()
        tax_product['id'] = tax['tax_class_id']
        tax_product['code'] = None
        tax_product['name'] = tax['title']
        tax_product['created_at'] = tax['date_added'] if (tax['date_added']) else '0000-00-00 00:00:00'
        #tax_product['created_at'] = tax['date_added'] ? tax['date_added'] : '0000-00-00 00:00:00'
        tax_product['updated_at'] = tax['date_modified'] if (tax['date_modified']) else '0000-00-00 00:00:00'
        #tax_product['updated_at'] = tax['date_modified']? tax['date_modified']:'0000-00-00 00:00:00'
        taxProduct = tax_product

        srcTaxRate = self.getListFromListByField(taxesExt['data']['tax_rates'], 'tax_class_id', tax['tax_class_id'])
        
        for  src_tax_rate in srcTaxRate.items():
            tax_zone_rate = self.constructTaxZoneRate()
                        
            tax_zone_rate['id'] = src_tax_rate['tax_rate_id']
            tax_zone_rate['name'] = src_tax_rate['name']
            tax_zone_rate['rate'] = src_tax_rate['rate']
            tax_zone_rate['priority'] = src_tax_rate['priority']
            tax_zone_rate['created_at'] = src_tax_rate['date_added'] if(src_tax_rate['date_added']) else '0000-00-00 00:00:00'
            tax_zone_rate['updated_at'] = src_tax_rate['date_modified'] if(src_tax_rate['date_modified']) else '0000-00-00 00:00:00'
            #tax_zone_rate['updated_at'] = src_tax_rate['date_modified'] ? src_tax_rate['date_modified'] : '0000-00-00 00:00:00'

            srcTaxZone = self.getListFromListByField(taxesExt['data']['zone_to_geo_zone'], 'geo_zone_id', src_tax_rate['geo_zone_id'])
           
            for src_tax_zone in srcTaxZone.items():
                 
                tax_zone_state = self.constructTaxZoneState()
                tax_zone_state['id'] = src_tax_zone['zone_id']
                tax_zone_state['name'] = src_tax_zone['name']
                tax_zone_state['state_code'] = src_tax_zone['zone_code']
                tax_zone_state['created_at'] = src_tax_zone['date_added'] if(src_tax_zone['date_added']) else '0000-00-00 00:00:00'
                tax_zone_state['updated_at'] = src_tax_zone['date_modified'] if(src_tax_zone['date_modified']) else '0000-00-00 00:00:00'
                #tax_zone_state['created_at'] = src_tax_zone['date_added'] ? src_tax_zone['date_added'] : '0000-00-00 00:00:00'
                #tax_zone_state['updated_at'] = src_tax_zone['date_modified']? src_tax_zone['date_modified'] : '0000-00-00 00:00:00'
                
                tax_zone_country = self.constructTaxZoneCountry()
                tax_zone_country['id'] = src_tax_zone['country_id']
                tax_zone_country['name'] = src_tax_zone['name']
                tax_zone_country['country_code'] = src_tax_zone['iso_code_2']
                tax_zone_country['created_at'] = src_tax_zone['date_added'] if(src_tax_zone['date_added']) else '0000-00-00 00:00:00'
                tax_zone_country['updated_at'] = src_tax_zone['updated_at'] if(src_tax_zone['updated_at']) else '0000-00-00 00:00:00'
                #tax_zone_country['created_at'] = src_tax_zone['date_added'] ? src_tax_zone['date_added'] : '0000-00-00 00:00:00'
                #tax_zone_country['updated_at'] = src_tax_zone['date_modified']? src_tax_zone['date_modified']:'0000-00-00 00:00:00'
                
                src_geo_zone = self.getRowFromListByField(taxesExt['data']['geo_zone'], 'geo_zone_id', src_tax_zone['geo_zone_id'])
                 
                tax_zone = self.constructTaxZone()
                tax_zone['id'] = src_tax_zone['geo_zone_id']
                tax_zone['name'] = src_geo_zone['name']
                tax_zone['description'] = src_geo_zone['description']
                tax_zone['country'] = tax_zone_country
                tax_zone['state'] = tax_zone_state
                tax_zone['rate'] = tax_zone_rate
                tax_zone['created_at'] = src_tax_zone['date_added'] if(src_tax_zone['date_added']) else '0000-00-00 00:00:00'
                tax_zone['updated_at'] = src_tax_zone['date_modified'] if(src_tax_zone['date_modified']) else '0000-00-00 00:00:00'
                #tax_zone['created_at'] = src_tax_zone['date_added']? src_tax_zone['date_added'] : '0000-00-00 00:00:00'
                #tax_zone['created_at'] = src_tax_zone['date_modified']? src_tax_zone['date_modified'] :'0000-00-00 00:00:00'
                taxZone = tax_zone
      
       
        tax_data = self.constructTax()
        tax_data['id'] = tax['tax_class_id']
        tax_data['name'] = tax['title']
        tax_data['created_at'] = tax['date_added'] if(tax['date_added']) else '0000-00-00 00:00:00'
        tax_data['updated_at'] = tax['date_modified'] if(tax['date_modified']) else '0000-00-00 00:00:00'
        #tax_data['created_at'] = tax['date_added'] ? tax['date_added']  : '0000-00-00 00:00:00'
        #tax_data['updated_at'] = tax['date_modified'] ? tax['date_modified'] : '0000-00-00 00:00:00'
        tax_data['tax_products'] = taxProduct
        tax_data['tax_zones'] = taxZone
                
        return {'result' : 'success',
            'msg' : '',
            'data' : tax_data}
    


    def getTaxIdImport(self,convert, tax, taxesExt):
        return convert['id']

    def checkTaxImport(self,convert, tax, taxesExt):
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        conn = self.connectdb()
        query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d and code_src = '%s'""" %(url_src, url_desc, self.TYPE_TAX, convert['id'], None, convert['code'])
        tax_exists = self.selectdb(conn,query)
        #tax_exists = self.selectMap(url_src, url_desc, self.TYPE_TAX, convert['id'], None, convert['code'])
        return  True if (tax_exists) else False

    def routerTaxImport(self,convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'} #taxImport - beforeTaxImport - additionTaxImport
        

    def beforeTaxImport(self,convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def taxImport(self,convert, tax, taxesExt):
        url_src = self._notice['src']['cart_url']
        url_target = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        taxClassId = []
        geoZoneId = zoneToGeoZoneId = taxProductImport = {}
        
        # tax class
        taxProductQueries = {}
        taxProduct = convert['tax_products']
        for key, tax_product in taxProduct.items():
            query_key = 'tax_product_' + key
            if(tax_product['id'] != None or tax_product['code'] != None):
                conn = self.connectdb()
                query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d""" %(url_src, url_target, self.TYPE_TAX_PRODUCT, tax_product['id'], tax_product['code'])
                tax_product_exists = self.selectdb(conn,query)
                #tax_product_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_PRODUCT, tax_product['id'], tax_product['code'])
                
                if(tax_product_exists):
                    taxClassId.append(tax_product_exists)
                    continue
                
            
            taxProductImport[query_key] = {'id' : tax_product['id'],
                'code' : tax_product['code']}
            fields_table = self.getAllColumnInTable('tax_class')
            data_insert = {'title' : tax_product['name'],
                'description' : tax_product['name'],
                'date_modified' : tax_product['updated_at'] ? self.convertStringToDatetime(tax_product['updated_at']) : self.datetimeNow("Y-m-d h:i:s"),
                'date_added' : tax_product['created_at'] ? self.convertStringToDatetime(tax_product['created_at']) : self.datetimeNow("Y-m-d h:i:s")}
            data_insert = self.syncFieldsInsert(data_insert, fields_table)
            query = "INSERT INTO tax_class "
            query += self.arrayToInsertCondition(data_insert)
            taxProductQueries[query_key] = {'type' : 'insert',
                'query' : query,
                'params' : {'insert_id' : True}}
        if(taxProductQueries):
            
            taxProductResponse = self.getConnectorData(url_query, {'serialize' : False,
                'query' : taxProductQueries})

            if(taxProductResponse == False or taxProductResponse['result'] != 'success'):
                return self.errorConnector()
            
        if(taxProductImport):
            for query_key , tax_product_data in taxProductImport.items():
                tax_class_id = taxProductResponse['data'][query_key]
                if(tax_class_id):
                    taxClassId.append(tax_class_id)
                    if(tax_product_data['id'] != None or tax_product_data['code'] != None):
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_PRODUCT, tax_product_data['id'], tax_class_id, tax_product_data['code'])
                        self.insertdb(conn,query)

                        #self.insertMap(url_src, url_target, self.TYPE_TAX_PRODUCT, tax_product_data['id'], tax_class_id, tax_product_data['code'])

        taxZone = convert['tax_zones']
        taxZoneId = self.duplicateFieldValueFromList(taxZone, 'id')
        for tax_zone_id in taxZoneId.items():
            taxZoneJoin = self.getListFromListByField(taxZone, 'id', tax_zone_id)
            conn = self.connectdb()
            query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d""" %(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone_id)
            self.selectdb(conn,query)
            #geo_zone_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone_id)
            
                
            geo_zone_id = None
            geo_zone_map_save = False
            if(geo_zone_exists)
                geo_zone_id = geo_zone_exists['id_desc']
                geo_zone_map_save = True

            
            for tax_zone in taxZoneJoin.items():
                if(geo_zone_id == False):
                    
                    fields_table = self.getAllColumnInTable('geo_zone')
                    data_insert = {'name' : tax_zone['name'],
                        'description' : '',
                        'date_modified' : self.convertStringToDatetime(tax_zone['updated_at']) if(tax_zone['updated_at']) else self.datetimeNow("Y-m-d h:i:s"),
                        'date_added' : self.convertStringToDatetime(tax_zone['created_at']) if(tax_zone['created_at']) else self.datetimeNow("Y-m-d h:i:s")}
                    
                    data_insert = self.syncFieldsInsert(data_insert, fields_table)
                    
                    geo_zone_query = "INSERT INTO geo_zone "
                    geo_zone_query += self.arrayToInsertCondition(data_insert)
                    geo_zone_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                'query' : geo_zone_query,
                                'params' : {'insert_id' : True}}})
                    if(geo_zone_import == False or geo_zone_import['result'] != 'success' or geo_zone_import['data'] == False ):
                        return self.errorConnector()
                
                    geo_zone_id = geo_zone_import['data']
                    conn = self.connectdb()
                    query = """insert into cartmigration_map(url_src,url_desc,type,id_src) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone['id'], tax_zone['code'], geo_zone_id)
                    self.insertdb(conn,query)
                    #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone['id'], tax_zone['code'], geo_zone_id)
                

                if(geo_zone_map_save == False):
                    tax_zone_country = tax_zone['country']
                    tax_zone_state = tax_zone['state']
                    zone_to_geo_zone_query = "INSERT INTO zone_to_geo_zone (country_id, zone_id, geo_zone_id, date_modified, date_added) VALUES ((SELECT country_id FROM countrie WHERE iso_code_2 = " . self.escape(tax_zone_country['code']) . "), (SELECT z.zone_id FROM zone AS z LEFT JOIN country AS c ON c.country_id = z.country_id WHERE c.iso_code_2 = " . self.escape(tax_zone_country['code']) . " AND z.code = " . self.escape(tax_zone_state['code']) . "), " . geo_zone_id . ", " . self.escape(tax_zone['updated_at'] ? self.convertStringToDatetime(tax_zone['updated_at']) : self.datetimeNow("Y-m-d h:i:s")) . ", " . self.escape(tax_zone['created_at']? self.convertStringToDatetime(tax_zone['created_at']):self.datetimeNow("Y-m-d h:i:s")) . ")"
                    zone_to_geo_zone_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                            'query' : zone_to_geo_zone_query}})


                tax_zone_rate = tax_zone['rate']
                tax_zone_rate_id = None
                if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                    conn = self.connectdb()
                    query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d and code_src = '%s'""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], None, tax_zone_rate['code'])
                    tax_zone_rate_exists = self.selectdb(conn,query)
                    #tax_zone_rate_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], None, tax_zone_rate['code'])
                    if(tax_zone_rate_exists):
                        tax_zone_rate_id = tax_zone_rate_exists
                    
                
                if(tax_zone_rate_id == False):
                    tax_zone_rate_queries = {}
                    fields_table = self.getAllColumnInTable('tax_rate')
                    for key, tax_class_id in taxClassId.items()
                        data_insert = {'geo_zone_id' : geo_zone_id,
                            'type' : 'P',
                            'rate' : tax_zone_rate['rate'],
                            'name' : tax_zone_rate['name'],
                            'date_modified' : self.convertStringToDatetime(tax_zone_rate['updated_at']) if (tax_zone_rate['updated_at']) else self.datetimeNow("Y-m-d h:i:s"), 
                            'date_added' : self.convertStringToDatetime(tax_zone_rate['created_at']) if (tax_zone_rate['created_at']) else self.datetimeNow("Y-m-d h:i:s")}
                            #'date_modified' : tax_zone_rate['updated_at'] ? self.convertStringToDatetime(tax_zone_rate['updated_at']) :  date("Y-m-d h:i:s"),
                            #'date_added' : tax_zone_rate['created_at'] ? self.convertStringToDatetime(tax_zone_rate['created_at']) : date("Y-m-d h:i:s"),
                        data_insert = self.syncFieldsInsert(data_insert, fields_table)
                        
                        tax_zone_rate_query = "INSERT INTO tax_rate "
                        tax_zone_rate_query = tax_zone_rate_query + self.arrayToInsertCondition(data_insert)
                        query_key = 'tax_zone_rate_' + key
                        tax_zone_rate_queries[query_key] = {'type' : 'insert',
                            'query' : tax_zone_rate_query,
                            'params' : {'insert_id' : True}}

                        
                    if(tax_zone_rate_queries):
                        tax_zone_rate_import = self.getConnectorData(url_query, {'serialize' : False,
                            'query' : tax_zone_rate_queries})
                        if(tax_zone_rate_import == False or tax_zone_rate_import['result'] != 'success' or tax_zone_rate_import['data'] == False):
                            return self.errorConnector()
                        
                        tax_zone_rate_id = tax_zone_rate_import['data']['tax_zone_rate_0']
                        
                        if(tax_zone_rate_id):
                            fields_table = self.getAllColumnInTable('tax_rule')
                            data_insert = {'tax_class_id' : taxClassId[0],
                                'priority' : tax_zone_rate['priority'],
                                'tax_rate_id' : tax_zone_rate_id}
                            data_insert = self.syncFieldsInsert(data_insert, fields_table)
           
                            tax_rule_query = "INSERT INTO tax_rule "
                            tax_rule_query += self.arrayToInsertCondition(data_insert)
                            tax_rule_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                    'query' : tax_rule_query}})
                            
                            if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                                conn = self.connectdb()
                                query = """insert into cartmigration_map(url_src,url_desc,type,id_src) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                                self.insertdb(conn,query)
                                #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX, convert['id'], 1, convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_target, self.TYPE_TAX, convert['id'], 1, convert['code'])

        return {'result' : 'success',
            'msg' : '',
            'data' : 1}

    def afterTaxImport(self,tax_id, convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def additionTaxImport(self,tax_id, convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    


    



    # manufactures
  
    def getManufacturersMainExport(self):
        last_id_f = self.get_id_last('Taxe')
        #id_src = self._notice['process']['manufacturers']['id_src']
        #limit = self._notice['setting']['manufacturers']
        manufacturers = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM manufacturer WHERE manufacturer_id > "  + last_id_f + " ORDER BY manufacturer_id ASC LIMIT " + last_id_f}})
        if(manufacturers == False or manufacturers['result'] != 'success'):
            return self.errorConnector()
        
        return manufacturers
    

    def getManufacturersExtExport(self,manufacturers):
        manufacturerId = self.duplicateFieldValueFromList(manufacturers['data'], 'manufacturer_id')
        manufacturer_id_con  = self.arrayToInCondition(manufacturerId)
        manufacturersExt = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'manufacturers_store' : {'type' : 'select',
                    'query' : "SELECT * FROM manufacturer_to_store WHERE manufacturer_id IN " + manufacturer_id_con}}})
        if(manufacturersExt == False or manufacturersExt['result'] != 'success'):
            return self.errorConnector()
        
        return manufacturersExt
    

    def convertManufacturerExport(self,manufacturer, manufacturersExt):
        manufacturer_data = self.constructManufacturer()
        manufacturer_data['id'] = manufacturer['manufacturer_id']
        manufacturer_data['name'] = manufacturer['name']
        manufacturer_data['image']['url'] = self.getUrlSuffix(self._notice['src']['config']['image_manufacturer'])
        manufacturer_data['image']['path'] = manufacturer['image']
        manufacturerInfo = self.getListFromListByField(manufacturersExt['data']['manufacturers_store'], 'manufacturer_id', manufacturer['manufacturer_id'])
        default_language = self._notice['src']['language_default']
        
        for language_id, language_label in self._notice['src']['languages'].items():
            manufacturer_language_data = self.constructManufacturerLang()
            manufacturer_language_data['name'] = manufacturer['name']
            manufacturer_data['languages'][language_id] = manufacturer_language_data
        
        return {'result' : 'success',
            'msg' : '',
            'data' : manufacturer_data}

    def getManufacturerIdImport(self,convert, manufacturer, manufacturersExt):
        return convert['id']

    def checkManufacturerImport(self,convert, manufacturer, manufacturersExt):
        return True if(self.getMapFieldBySource(self.TYPE_MANUFACTURER, convert['id'], convert['code'])) else False
        #return self.getMapFieldBySource(self.TYPE_MANUFACTURER, convert['id'], convert['code']) ? True : False

    def routerManufacturerImport(self,convert, manufacturer, manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'manufacturerImport'}

    def beforeManufacturerImport(self,convert, manufacturer, manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        

    def manufacturerImport(self,convert, manufacturer, manufacturersExt):
        url_src = self._notice['src']['cart_url']
        url_target = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
       
        url_image = self.getConnectorUrl('image')
        manufacturers_image = ' '
        if(convert['image']['path']):
            image_process = self.processImageBeforeImport(convert['image']['url'], convert['image']['path'])
            image_import = self.getConnectorData(url_image, {'images' : {'mi' : {'type' : 'download',
                        'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_manufacturer'].'catalog/'),
                        'params' : {'url' : image_process['url'],
                            'rename' : True}}}})
            
            if(image_import and image_import['result'] == 'success'):
                image_import_path = image_import['data']['mi']
                if(image_import_path):
                    manufacturers_image = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_manufacturer'])
                
        fields_table = self.getAllColumnInTable('manufacturer')
        
        data_insert = {'name' : convert['name'],
            'image' : manufacturers_image}
        data_insert = self.syncFieldsInsert(data_insert, fields_table)
        
        manufacturer_query = "INSERT INTO manufacturer "
        manufacturer_query += self.arrayToInsertCondition(data_insert)        
        manufacturer_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : manufacturer_query,
                'params' : {'insert_id' : True}}})
        
    if(manufacturer_import == False or manufacturer_import['result'] != 'success' or manufacturer_import['data'] == False):
        ##var_dump(manufacturer_query)
        return {'result' : 'error',
            'msg' : 'warning'}
        manufacturer_id = manufacturer_import['data']
        

        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_MANUFACTURER, convert['id'], manufacturer_id, convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_target, self.TYPE_MANUFACTURER, convert['id'], manufacturer_id, convert['code'])
        return {'result' : 'success',
            'msg' : '',
            'data' : manufacturer_id}
        

    def afterManufacturerImport(self,manufacturer_id, convert, manufacturer, manufacturersExt):
        url_query = self.getConnectorUrl('query')
        manufacturer_info_queries = {}
        ##category to store
        manufacturer_to_store_query = "INSERT INTO manufacturer_to_store "
        manufacturer_to_store_query = manufacturer_to_store_query +  self.arrayToInsertCondition({'manufacturer_id' : manufacturer_id,
            'store_id' : 0})

        manufacturer_to_store_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : manufacturer_to_store_query}})
        ## seo 
        if(self.iset(convert,'url_key')):
            manufacturer_seo_query = "INSERT INTO url_alias "
            manufacturer_seo_query = manufacturer_seo_query + self.arrayToInsertCondition({'query' : 'manufacturer_id='.manufacturer_id,
                'keyword' : convert['url_key']})

            manufacturer_seo_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                    'query' : manufacturer_seo_query}})
       
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        

    def additionManufacturerImport(self,manufacturer_id, convert, manufacturer, manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
    














    #catergory


    def getCategoriesMainExport(self):
        last_id_f = self.get_id_last('Category')
        #id_src = self._notice['process']['categories']['id_src']
        #limit = self._notice['setting']['categories']
        categories = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM category WHERE category_id > " + last_id_f + " ORDER BY category_id ASC LIMIT " + last_id_f}})
        if(categories == False or categories['result'] != 'success'):
            return self.errorConnector()
        
        return categories
    

    def getCategoriesExtExport(self,categories):
        categoriesId = self.duplicateFieldValueFromList(categories['data'], 'category_id')
        category_id_con = self.arrayToInCondition(categoriesId)
        category_ids_query = self._arrayToInConditionCategory(categoriesId)
        categories_ext_queries = {'categories_description' : {'type' : 'select',
                'query' : "SELECT * FROM category_description WHERE category_id IN " + category_id_con},
            'url_alias' : {'type' : 'select',
                "query" : "SELECT * FROM url_alias WHERE query IN {category_ids_query}"}}
        categoriesExt = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : categories_ext_queries})
        if(categoriesExt == False or categoriesExt['result'] != 'success'):
            return self.errorConnector()
        return categoriesExt
    

    def convertCategoryExport(self,category, categoriesExt):
        category_data = self.constructCategory()
        parent = self.constructCategoryParent()
        if(category['parent_id'])
            parent = self.getCategoryParent(category['parent_id']) ## chua co function
            if(parent['result'] != 'success')
                response = self._defaultResponse()
                response['result'] = 'warning'
                response['msg'] = self.consoleWarning("Could not convert.")
                return response
            
            parent = parent['data']
        else:
            parent['id'] = 0
        
        category_data['id'] = category['category_id']
        category_data['parent'] = parent
        category_data['active'] = True if(category['status']) else False
        #category_data['active'] = category['status'] ? True : False
        category_data['image']['url'] = self.getUrlSuffix(self._notice['src']['config']['image_category'])
        category_data['image']['path'] = category['image']
        category_data['sort_order'] = category['sort_order']
        category_data['created_at'] = category['date_added'] if(category['date_added']) else '0000-00-00 00:00:00'
        #category_data['created_at'] = category['date_added'] ? category['date_added'] : '0000-00-00 00:00:00'
        category_data['updated_at'] = category['date_modified'] if(category['date_modified']) else '0000-00-00 00:00:00'
        #category_data['updated_at'] = category['date_modified'] ? category['date_modified'] : '0000-00-00 00:00:00'
        category_data['category'] = category
        category_data['categoriesExt'] = categoriesExt

        categoryDescription = self.getListFromListByField(categoriesExt['data']['categories_description'], 'category_id', category['category_id'])
        language_default = self._notice['src']['language_default']
        categoryDescriptionDef = self.getRowFromListByField(categoryDescription, 'language_id', language_default)
        if(categoryDescriptionDef == False):
            categoryDescriptionDef = categoryDescription[0]

        category_data['name'] = html.unescape(categoryDescriptionDef['name'])
        category_data['meta_title'] = html.unescape(categoryDescriptionDef['meta_title'])
        category_data['meta_keyword'] = categoryDescriptionDef['meta_keyword']
        category_data['meta_description'] = html.unescape(categoryDescriptionDef['meta_description'])
        
        for  language_id, language_label in self._notice['src']['languages'].items():
            category_language_data = self.constructCategoryLang()
            categoryDescriptionLang = self.getRowFromListByField(categoryDescription, 'language_id', language_id)
            if(categoryDescriptionLang):
                category_language_data['name'] = html.unescape(categoryDescriptionLang['name'])
                category_language_data['description'] = categoryDescriptionLang['description'].replace('\n','<br />\n')
                category_language_data['meta_title'] = categoryDescriptionLang['meta_title']
                category_language_data['meta_keyword'] = categoryDescriptionLang['meta_keyword']
                category_language_data['meta_description'] = categoryDescriptionLang['meta_description']
                category_data['languages'][language_id] = category_language_data
                category_data['languages'][language_id] = category_language_data
            

        return {'result' : 'success',
            'msg' : '',
            'data' : category_data}
        

    def getCategoryIdImport(self,convert, category, categoriesExt):
        return convert['id']

    def checkCategoryImport(convert, category, categoriesExt):
        return True if(self.getMapFieldBySource(self.TYPE_CATEGORY, convert['id'], convert['code'])) else False
        #return self.getMapFieldBySource(self.TYPE_CATEGORY, convert['id'], convert['code']) ? True : False

    def routerCategoryImport(self,convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'categoryImport'}

    def beforeCategoryImport(self,convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        

    def categoryImport(self,convert, category, categoriesExt):
        
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        url_image = self.getConnectorUrl('image')
        
        categories_image = ' '
        if(convert['image']['url']):
            image_process = self.processImageBeforeImport(convert['image']['url'], convert['image']['path'])
            image_import = self.getConnectorData(url_image, {'images' : {'ci' : {'type' : 'download',
                        'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_category'].'catalog/'),
                        'params' : {'url' : image_process['url'],
                            'rename' : True}}}})

            if(image_import and image_import['result'] == 'success'):
                image_import_path = image_import['data']['ci']
                if(image_import_path):
                    categories_image = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_category'])
                
            
        
       
        if(convert['parent'] and (convert['parent']['id'] or convert['parent']['code'])):
           
            parent_import = self._importCategoryParent(convert['parent'])
            if(parent_import['result'] != 'success'):
                response = self._defaultResponse()
                response['result'] = 'warning'
                response['msg'] = 'Could not import'
                return response
            
            parent_id = parent_import['data']
        else:
            parent_id = 0
        
        fields_table = self.getAllColumnInTable('category')
        data_insert ={'image' : categories_image,
            'parent_id' : parent_id,
            'top' : 0,
            'column' : 0,
            'status' : 1 if(convert['active']) else 0,
            'sort_order' : convert['sort_order'] if(convert['sort_order']) else 0,
            'date_added' : self.convertStringToDatetime(convert['created_at']) if(convert['created_at']) else self.datetimeNow("Y-m-d h:i:s"),
            'date_modified' : self.convertStringToDatetime(convert['updated_at']) if(convert['updated_at']) else self.datetimeNow("Y-m-d h:i:s")}
            #'status' : (convert['active']) ? 1 : 0,
            #'sort_order' : convert['sort_order'] ? convert['sort_order'] : 0,
            #'date_added' : convert['created_at'] ? self.convertStringToDatetime(convert['created_at']) :date("Y-m-d h:i:s"),
            #'date_modified' : (convert['updated_at']) ? self.convertStringToDatetime(convert['updated_at']) : date("Y-m-d h:i:s"),
    
        data_insert = self.syncFieldsInsert(data_insert, fields_table)
        
        category_query = "INSERT INTO category "
        category_query += self.arrayToInsertCondition(data_insert)
                
        category_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : category_query,
                'params' : {'insert_id' : True}}})
        
        if(category_import == False or category_import['result'] != 'success' or category_import['data'] == False):
            ##warning
            # var_dump(category_query)
            return {'result' : 'error',
                'msg' : 'warning'}

        category_id = category_import['data']
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_CATEGORY, convert['id'], category_id, convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_desc, self.TYPE_CATEGORY, convert['id'], category_id, convert['code'])
        return {'result' : 'success',
            'msg' : '',
            'data' : category_id}
        

    def afterCategoryImport(self,category_id, convert, category, categoriesExt):
        url_query = self.getConnectorUrl('query')
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        #category to store
        category_to_store_query = "INSERT INTO category_to_store "
        category_to_store_query = category_to_store_query + self.arrayToInsertCondition({'category_id' : category_id,
            'store_id' : 0})

        category_to_store_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : category_to_store_query}})
        
        if(category_to_store_import == False or category_to_store_import['result'] != 'success'):
            ## warning
            return self.errorConnector()
        
        level = 0
        cpath_queries = {}
        if(self.iset(convert['parent'],'id') or self.iset(convert['parent'],'code')):
            conn = self.connectdb()
            query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d and code_src = '%s'""" %(url_src, url_desc, self.TYPE_CATEGORY, convert['parent']['id'], None, convert['parent']['code'])
            parent_exists = self.selectdb(conn,query)
            #parent_exists = self.selectMap(url_src, url_desc, self.TYPE_CATEGORY, convert['parent']['id'], None, convert['parent']['code'])
            if(parent_exists)
                parentId = parent_exists

                category_path = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                        'query' : "SELECT * FROM category_path WHERE category_id = 'parentId' AND path_id = 'parentId'"}})
                if(category_path or category_path['result'] != 'success'):
                    # warning
                    return self.errorConnector()
                

                if(category_path['data'])
                    level = category_path['data'][0]['level'] + 1
                    category_paths = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                            'query' : "SELECT * FROM category_path WHERE category_id = 'parentId'"}})
                    if(category_paths == False or category_paths['result'] != 'success'):
                        ## warning
                        return self.errorConnector()
                    
                    maxLevel = 0
                    if(category_paths['data']):

                        for cpath in category_paths['data'].items():
                            query =  "INSERT INTO category_path "
                            query = query +  self.arrayToInsertCondition({'category_id' : category_id,
                                'path_id' : cpath['path_id'],
                                'level' : cpath['level']})
                            cpath_queries[] = {'type' : 'insert',
                                'query' : query}
                            
        
       
        category_path_query = "INSERT INTO category_path "
        category_path_query = category_path_query + self.arrayToInsertCondition({'category_id' : category_id,
            'path_id' : category_id,
            'level' : level})
    
         category_path = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'insert',
                'query' : category_path_query}})
        if(category_path == False or category_path['result'] != 'success'):
            return self.errorConnector()
        
        if(cpath_queries):
            cpaths = self.getConnectorData(url_query, {'serialize' : False,
                'query' : cpath_queries})
        
        categories_description_queries = {}
        fields_table = self.getAllColumnInTable('category_description')
        for src_language_id, target_language_id in self._notice['map']['languages'].items():
            categoryLang = convert['languages'][src_language_id] if(self.iset(convert['languages'],src_language_id)) else convert
            #categoryLang = isset(convert['languages'][src_language_id]) ? convert['languages'][src_language_id] : convert
            
            if(categoryLang):
                key = 'categories_description_' + src_language_id
                categories_description_query = "INSERT INTO category_description "
                
                data_insert ={'category_id' : category_id,
                    'language_id' : target_language_id,
                    'name' : categoryLang['name'],
                    'description' : categoryLang['description'].replace('\n','<br />\n') if categoryLang['description'] else '',
                    #'description' : categoryLang['description'] else '', ? nl2br(categoryLang['description']) : '',
                    'meta_title' : categoryLang['meta_title'] if(categoryLang['meta_title']) else '',
                    #'meta_title' : categoryLang['meta_title'] ? categoryLang['meta_title'] : '',
                    'meta_description' : categoryLang['meta_description'] if (categoryLang['meta_description']) else '',
                    
                    #'meta_description' : categoryLang['meta_description'] ? categoryLang['meta_description'] : '',
                    'meta_keyword' : categoryLang['meta_keyword'] if(categoryLang['meta_keyword']) else ''}
                    #'meta_keyword' : categoryLang['meta_keyword'] ? categoryLang['meta_keyword'] : '',
                
                data_insert = self.syncFieldsInsert(data_insert, fields_table)
        
                categories_description_query += self.arrayToInsertCondition(data_insert)
                categories_description_queries[key] = {'type' : 'insert',
                    'query' : categories_description_query}
            
            
        ##var_dump(categories_description_queries)exit
        if(categories_description_queries):
            categories_description_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : categories_description_queries})

        
        ## SEO
        '''if(self._notice["config"]['seo_plugin'] and self._notice["config"]["seo"]):
            seo_queries = {}
            seo_name = self._notice["config"]["seo_plugin"]
            model_seo = Bootstrap.getModel(seo_name)
            data_seo = model_seo->getCategoriesSeoExport(this, category, categoriesExt)
            k = 1
            foreach (data_seo as seo){
               seo_query = "INSERT INTO url_alias "
                    seo_query += self.arrayToInsertCondition(array(
                        'query' : 'category_id='.category_id,
                        'keyword' : seo['request_path']
                    ))
                    query_key = "seo" . k
                    seo_queries[query_key] = array(
                        'type' : 'insert',
                        'query' : seo_query
                    )
                    k++
            }
            if(seo_queries){
                product_seo_import = self.getConnectorData(url_query, array(
                    'serialize' : False,
                    'query' : serialize(seo_queries)
                ))
            }
        }'''

        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    

    def additionCategoryImport(self,category_id, convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    








    #product



    def getProductsMainExport(self):
        #id_src = self._notice['process']['products']['id_src']
        #limit = self._notice['setting']['products']
        last_id_f = self.get_id_last('Product')
        products = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM product WHERE product_id > " + last_id_f + " ORDER BY product_id ASC LIMIT " + last_id_f}})
        if(products == False or products['result'] != 'success'):
            return self.errorConnector()
        
        return products
    

    def getProductsExtExport(self,products):
        url_query = self.getConnectorUrl('query')
        
        productId = self.duplicateFieldValueFromList(products['data'], 'product_id')
        manufacturerId = self.duplicateFieldValueFromList(products['data'], 'manufacturer_id')
        
        manufacturer_id_con = self.arrayToInCondition(manufacturerId)
        product_id_con = self.arrayToInCondition(productId)
        product_ids_query = self._arrayToInConditionProduct(productId)
    
        product_ext_queries = {'products_description' : {'type' : "select",
                'query' : "SELECT * FROM product_description WHERE product_id IN " + product_id_con},
            'products_discount' : {'type' : "select",
                'query' : "SELECT * FROM product_discount WHERE product_id IN  " + product_id_con},
            'products_option_value' : {'type' : "select",
                'query' : "SELECT * FROM product_option_value WHERE product_id IN " + product_id_con},
            'products_related' : {'type' : "select",
                'query' : "SELECT * FROM product_related WHERE product_id IN  " + product_id_con},

            'products_to_categories' : {'type' : 'select',
                'query' : "SELECT * FROM product_to_category WHERE product_id IN " + product_id_con},
            'products_images' : {'type' : 'select',
                'query' : "SELECT * FROM product_image WHERE product_id IN " + product_id_con},
            'products_attributes' : {'type' : 'select',
                'query' : "SELECT * FROM product_attribute WHERE product_id IN " + product_id_con},
            'products_options' : {'type' : 'select',
                'query' : "SELECT po.*, o.type FROM product_option AS po LEFT JOIN `option` AS o ON po.option_id = o.option_id WHERE po.product_id IN " + product_id_con},
            'specials' : {'type' : 'select',
                'query' : "SELECT * FROM product_special WHERE product_id IN " + product_id_con},
            'manufacturers' : {'type' : 'select',
                'query' : "SELECT manufacturer_id, name FROM manufacturer WHERE manufacturer_id IN " + manufacturer_id_con},
            "url_alias" : {'type' : 'select',
                "query" : "SELECT * FROM url_alias WHERE query IN {product_ids_query}"}}
        
        productsExt = self.getConnectorData(url_query, {'serialize' : False,
            'query' : product_ext_queries})
        if(productsExt  == False or productsExt['result'] != 'success')
            return self.errorConnector()
        

        productOptionIds = self.duplicateFieldValueFromList(productsExt['data']['products_options'], 'option_id')
        attributeIds = self.duplicateFieldValueFromList(productsExt['data']['products_attributes'], 'attribute_id')
        option_ids_con = self.arrayToInCondition(productOptionIds)
        attribute_ids_con = self.arrayToInCondition(attributeIds)
        product_ext_rel_queries = {'option_description' : {'type' : 'select',
                'query' : "SELECT o.*, od.* FROM `option` as o LEFT JOIN `option_description` as od ON od.option_id = o.option_id WHERE o.option_id IN " + option_ids_con},
            'option_value_description' : {'type' : 'select',
                'query' : "SELECT * FROM `option_value_description` as ov WHERE ov.option_id IN " + option_ids_con},
            'attribute_description' : {'type' : 'select',
                'query' : "SELECT * FROM attribute_description WHERE attribute_id IN " + attribute_ids_con}}
        
        productsExtRel = self.getConnectorData(url_query, {'serialize' : False,
            'query' : product_ext_rel_queries})
        if(productsExtRel False or productsExtRel['result'] != 'success'):
            return self.errorConnector()
        
        productsExt = self.syncConnectorObject(productsExt, productsExtRel)
        return productsExt
    

    def convertProductExport(self.product, productsExt):
        product_data = self.constructProduct()
        product_data['id'] = product['product_id']
        product_data['sku'] = product['model']
        product_data['price'] = product['price']
        product_data['weight'] = product['weight']
        product_data['status'] = product['status']
        product_data['qty'] = product['quantity']
        product_data['created_at'] = product['date_added'] if product['date_added'] else '0000-00-00 00:00:00'
        #product_data['created_at'] = product['date_added'] ? product['date_added'] : '0000-00-00 00:00:00'
        product_data['updated_at'] = product['date_modified'] if product['date_modified'] else '0000-00-00 00:00:00'
        #product_data['updated_at'] = product['date_modified'] ? product['date_modified'] : '0000-00-00 00:00:00'
        product_data['weight'] = product['weight']
        product_data['length'] = product['length']
        product_data['width'] = product['width']
        product_data['height'] = product['height']
        
        productDescription = self.getListFromListByField(productsExt['data']['products_description'], 'product_id', product['product_id'])
        language_default = self._notice['src']['language_default']
        productDescriptionDef = self.getRowFromListByField(productDescription, 'language_id', language_default)
        
        if(productDescriptionDef == False):
            productDescriptionDef = productDescription[0]

        product_data['name'] = productDescriptionDef['name'].replace('\n','<br />\n')
        product_data['description'] = productDescriptionDef['description'].replace('\n','<br />\n')
        product_data['short_description'] = productDescriptionDef['description'].replace('\n','<br />\n')
        product_data['meta_title'] = productDescriptionDef['meta_title'].replace('\n','<br />\n')
        product_data['meta_description'] = productDescriptionDef['meta_description']
        product_data['meta_keyword'] = productDescriptionDef['meta_keyword'] 
        product_data['tags'] = productDescriptionDef['tag']
        url_product_image = self.getUrlSuffix(self._notice['src']['config']['image_product'])
        product_data['image']['url'] = url_product_image
        product_data['image']['path'] = product['image']

        productImage = self.getListFromListByField(productsExt['data']['products_images'], 'product_id', product['product_id'])
        if(productImage):
            for product_image in productImage.items():
                product_image_data = self.constructProductImage()
                product_image_data['url'] = url_product_image
                product_image_data['path'] = product_image['image']
                product_data['images'][] = product_image_data


        special = self.getRowFromListByField(productsExt['data']['specials'], 'product_id', product['product_id'])
        if(special):
            product_data['special_price']['price'] = special['price']
            product_data['special_price']['start_date'] = special['date_start']
            product_data['special_price']['end_date'] = special['date_end']
        

        product_data['tax']['id'] = product['tax_class_id']

         if(product['manufacturer_id']):
            product_data['manufacturer']['id'] = product['manufacturer_id']
            manufacturer = self.getRowFromListByField(productsExt['data']['manufacturers'], 'manufacturer_id', product['manufacturer_id'])
            if(manufacturer)
                product_data['manufacturer']['name'] = manufacturer['name']
            
        
        productCategory = self.getListFromListByField(productsExt['data']['products_to_categories'], 'product_id', product['product_id'])
        if(productCategory):
            for product_category in productCategory.items():
                product_category_data = self.constructProductCategory()
                product_category_data['id'] = product_category['category_id']
                product_data['categories'][] = product_category_data
            
        

        for product_description in productDescription.items():
            product_language_data = self.constructProductLang()
            product_language_data['name'] = product_description['name'].replace('\n','<br />\n')
            product_language_data['description'] = product_description['description'].replace('\n','<br />\n')
            product_language_data['short_description'] = .replace('\n','<br />\n')product_description['description'].replace('\n','<br />\n')
            product_language_data['meta_title'] = .replace('\n','<br />\n')product_description['meta_title'].replace('\n','<br />\n')
            product_language_data['meta_description'] = product_description['meta_description'].replace('\n','<br />\n')
            product_language_data['meta_keyword'] = product_description['meta_keyword']        
            language_id = product_description['language_id']
            product_data['languages'][language_id] = product_language_data
        
        
        productOptions = self.getListFromListByField(productsExt['data']['products_options'], 'product_id', product['product_id'])
        if(productOptions):
            childsData = []
            comb = self.constructChildProduct()
            comb['name'] = productDescriptionDef['name']
            comb['qty'] = product['quantity']
            comb['sku'] = product['model']
            comb['price'] = product['price']
            comb['languages'] = product_data['languages']
            childsData.append(comb)
            optionId = self.duplicateFieldValueFromList(productOptions, 'option_id')
            productOptionValues = self.getListFromListByField(productsExt['data']['products_option_value'], 'product_id', product['product_id'])
            for option in productOptions.items():
                
                option_data = self.constructProductOption()
                option_data['id'] = option['option_id']

                productOptionDesc = self.getListFromListByField(productsExt['data']['option_description'], 'option_id', option['option_id'])
                productOptionDef = self.getRowFromListByField(productOptionDesc, 'language_id', language_default)
                
                if(productOptionDef == False):
                    productOptionDef = productOptionDesc[0]
                
                option_data['option_name'] = productOptionDef['name']
                option_data['option_type'] = option['type']
                option_data['required'] = option['required']
                for product_option in productOptionDesc.items():
                    option_language_data = self.constructProductOptionLang()
                    option_language_data['option_name'] = product_option['name']
                    language_id = product_option['language_id']
                    option_data['option_languages'][language_id] = option_language_data
                

                _productOptionValues = self.getListFromListByField(productOptionValues, 'option_id', option['option_id'])
               
                newchilds = []
                for optionValues in _productOptionValues.items():
                    dataOptionVarianProduct = self.constructChildProduct()
                    option_value_data = self.constructProductOptionValue()
                    option_value_data['id'] = optionValues['option_value_id']

                    productOptionValue = self.getListFromListByField(productsExt['data']['option_value_description'], 'option_value_id', optionValues['option_value_id'])
                    productOptionValueDef = self.getRowFromListByField(productOptionValue, 'language_id', language_default)
                    if(productOptionValueDef == False):
                        productOptionValueDef = productOptionValue[0]
                    
                    option_value_data['option_value_name'] = productOptionValueDef['name']

                    for product_option_value in productOptionValue.items():
                        option_value_language_data = self.constructProductOptionValueLang()
                        option_value_language_data['option_value_name'] = product_option_value['name']
                        language_id = product_option_value['language_id']
                        option_value_data['option_value_languages'][language_id] = option_value_language_data
                    
                
                    option_value_data['price'] = optionValues['price']
                    option_value_data['price_prefix'] = optionValues['price_prefix']
                    for childData in childsData.items():
                        child = self.constructChildProduct()
                        childAttr = self.constructChildProductAttribute()
                        childAttr['option_id'] = option['option_id']
                        childAttr['option_type'] = option['type']
                        childAttr['option_name'] = productOptionDef['name'].replace('\n','<br />\n')
                        
                        childAttr['option_languages'] = option_data['option_languages']
                        
                        childAttr['option_value_id'] = optionValues['option_value_id']
                        
                        childAttr['option_value_name'] = productOptionValueDef['name']
                        childAttr['option_value_languages'] = option_value_data['option_value_languages']
                        childAttr['price'] = optionValues['price']
                        childAttr['price_prefix'] = optionValues['price_prefix']
                        child['name'] = childData['name'] +' - '+ option_value_data['option_value_name']
                        child['sku'] = childData['sku']+'-'+self.joinTextToKey(option_value_data['option_value_name'])
                        child['qty'] = optionValues['quantity'] if (childData['qty'] > optionValues['quantity']) else childData['qty']
                        #child['qty'] = (childData['qty'] > optionValues['quantity']) ? optionValues['quantity'] : childData['qty']
                        if(option_value_data['price_prefix'] == '-'):
                            child['price'] = childData['price'] - optionValues['price']
                        else:
                            child['price'] = childData['price'] + optionValues['price']
                        
                        for langId, langData in childData['languages'].items():
                            child_language_data = self.constructChildProductLang()
                            child_language_data['name'] = (langData['name'] +' - ' + option_value_data['option_value_languages'][langId]['option_value_name']).replace('\n','<br />\n')
                            child_language_data['description'] = langData['description'].replace('\n','<br />\n')
                            child_language_data['short_description'] = langData['description'].replace('\n','<br />\n')
                            child_language_data['meta_title'] = langData['meta_title'].replace('\n','<br />\n')
                            child_language_data['meta_description'] = langData['meta_description']
                            child_language_data['meta_keyword'] = langData['meta_keyword']
                            child['languages'][langId] = child_language_data
                        
                        child['attributes'] = childData['attributes']
                        child['attributes'][] = childAttr
                        newchilds.append(child)
                    
                    option_data['values'].append(option_value_data)
                
                childsData = newchilds
                product_data['options'].append(option_data)
                
            
            product_data['children'] = childsData
          
        ##  #var_dump(product_data['children'])exit
        ## Attributes
        productAttributes =  self.getListFromListByField(productsExt['data']['products_attributes'], 'product_id', product['product_id'])
        if(productAttributes):
            for attribute in productAttributes.items():
                attribute_data = self.constructProductAttribute()
                attribute_data['option_id'] = attribute['attribute_id']
                productAttributeDesc = self.getListFromListByField(productsExt['data']['attribute_description'], 'attribute_id', attribute['attribute_id'])
                productAttributeDef = self.getRowFromListByField(productAttributeDesc, 'language_id', language_default)
                if(productAttributeDef == False):
                    productAttributeDef = productAttributeDesc[0]
                
                attribute_data['option_name'] = productAttributeDef['name']
##                for product_option_value in productOptionValue.items():
##                        option_value_language_data = self.constructProductAttributeValueLang()
##                        option_value_language_data['option_value_name'] = product_option_value['name']
##                        language_id = product_option_value['language_id']
##                        option_value_data['option_value_languages'][language_id] = option_value_language_data
##                    }   
                attribute_data['option_value_name'] = attribute['text']
##                attribute_data['price'] =       
##                attribute_data['price_prefix'] =       
##                attribute_data['option_languages'] =      
##                attribute_data['option_value_languages'] =       

                attribute_data['option_type'] = 'text'          
                product_data['attributes'].append(attribute_data)
            
        
        return {'result' : 'success',
            'msg' : '',
            'data' : product_data}
    

    def getProductIdImport(self,convert, product, productsExt):
        return convert['id']
    

    def checkProductImport(self,convert, product, productsExt):
        return True if self.getMapFieldBySource(self.TYPE_PRODUCT, convert['id'], convert['code']) else False
        #return self.getMapFieldBySource(self.TYPE_PRODUCT, convert['id'], convert['code']) ? True : False
    

    def routerProductImport(self,convert, product, productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'productImport'}

    def beforeProductImport(self,convert, product, productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def productImport(self,convert, product, productsExt):
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')

        url_image = self.getConnectorUrl('image')
        ##var_dump(url_image)exit()
        ## import image
        products_image = ' '
        found_image = False
        ##var_dump(convert['image'])exit()
        if(convert['image']['url']):
            if(isinstance(convert['image']['path'],(frozenset, list, set, tuple)) == False):
                paths = [(convert['image']['path'])]
            else:
                paths = convert['image']['path']
            
            for k, path in paths.items():
                image_process = self.processImageBeforeImport(convert['image']['url'], path)
                ##var_dump(url_image)
                ##var_dump(image_process['path'])
                ##var_dump(self._notice['target']['config']['image_product'].'catalog/')
                ##var_dump(image_process['url'])
                ##exit()
                if(Image.open(image_process['url']).size):
                    image_import = self.getConnectorData(url_image, {'images' : {'pi' : {'type' : 'download',
                                'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_product'].'catalog/'),
                                'params' : {'url' : image_process['url'],
                                    'rename' : True}}}})

                    ##var_dump(image_import)exit()
                    ##var_dump(self.addPrefixPath(image_process['path'], 'image/catalog/'))exit()
                    ##var_dump(image_import)exit()
                    if(image_import and image_import['result'] == 'success' and image_import['data']['pi']):
                        ##var_dump(self._notice['target']['config']['image_product'])exit()
                        image_import_path = image_import['data']['pi']
                        products_image = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_product'])
                        found_image = True
                        break

        tax_product_id = 0
        if(convert['tax']['id'] or convert['tax']['code']):
            tax_product_id = self.getMapFieldBySource(self.TYPE_TAX_PRODUCT, convert['tax']['id'], convert['tax']['code'])
            if(tax_product_id == False):
                tax_product_id = 0
            
        

        manufacturer_id = 0
        if(convert['manufacturer']['id'] or convert['manufacturer']['code']):
            manufacturer_id = self.getMapFieldBySource(self.TYPE_MANUFACTURER, convert['manufacturer']['id'], convert['manufacturer']['code'])
            if(manufacturer_id == False):
                manufacturer_id = 0
            
        
        if(self.iset(convert,'stock_status_id')):
            stock_status_id = convert['stock_status_id']
        
        else if:
            ((convert['manage_stock'] and convert['qty'] > 0) or convert['manage_stock'] == False):
            stock_status_id = 7
        else if:
            (convert['manage_stock'] and convert['qty'] <= 0)
            stock_status_id = 5
        else:
            stock_status_id = 0
        
        if(self.iset(convert,'is_in_stock')):
            stock_status_id = 7
        
        date_available= '2016-01-01'
        if(self.iset(convert,'date_available')):
            date_available = convert['date_available']
        
        fields_table = self.getAllColumnInTable('product')
        data_insert = {'quantity' : convert['qty'] if(convert['manage_stock']) else 9999,
            'model' : convert['sku'] if convert['sku'] else '',
            'image' : products_image,
            'price' : convert['price'],
            'date_available' : date_available,
            'date_added' : convert['created_at'] if(convert['created_at']) else  self.datetimeNow("Y-m-d h:i:s"),
            'date_modified' : (convert['updated_at']) if(convert['updated_at']) else self.datetimeNow("Y-m-d h:i:s"),
            'weight' : convert['weight'] if convert['weight'] else 0,
            'length' : convert['length'] if convert['length'] else 0,
            'width' : convert['width'] if convert['width'] else 0,
            'height' : convert['height'] if convert['height'] else 0,
            'status' : 1 if(convert['status']) else 0,
            'tax_class_id' : tax_product_id,
            'stock_status_id' : stock_status_id,
            'manufacturer_id' : manufacturer_id}
        data_insert = self.syncFieldsInsert(data_insert, fields_table)

        product_query = "INSERT INTO product "
        product_query += self.arrayToInsertCondition(data_insert)
        product_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : product_query,
                'params' : {'insert_id' : True}}})
                
        
        if(product_import == False or product_import['result'] != 'success' or product_import['data'] == False):
        #    # warning
        ##             echo product_queryexit
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        product_id = product_import['data']
        if(product_id):
            echo product_queryexit
            return self.errorConnector()
        
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_PRODUCT, convert['id'], product_id, convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_desc, self.TYPE_PRODUCT, convert['id'], product_id, convert['code'])
        return {'result' : 'success',
            'msg' : '',
            'data' : product_id}
        
    

    def afterProductImport(self,product_id, convert, product, productsExt):

        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        url_image = self.getConnectorUrl('image')
        ##product to store
        product_to_store_query = "INSERT INTO product_to_store "
        product_to_store_query += self.arrayToInsertCondition({'product_id' : product_id,
            'store_id' : 0})

        product_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : product_to_store_query}})

        
        if(product_import == False or product_import['result'] != 'success'):
            ## warning
            return self.errorConnector()
        
        ## description
        fields_table = self.getAllColumnInTable('product_description')
        products_description_queries = {}
        for src_language_id, desc_language_id in self._notice['map']['languages'].items():
            productDescriptionLang = convert['languages'][src_language_id] if (self.iset(convert['languages'],src_language_id)) else convert
            
            data_insert = {'product_id' : product_id,
                'language_id' : desc_language_id,
                'name' : productDescriptionLang['name'],
                'description' : self.changeImgSrcInText(productDescriptionLang['description'], self._notice['config']['img_des'], self._notice['target']['config']['image_product']),#nl2br()
                'tag' : convert['tags'] if convert['tags'] else ' ',
                'meta_title' : convert['meta_title'] if convert['meta_title'] else ' ',
                'meta_description' : convert['meta_description'] if convert['meta_description'] else ' ',
                'meta_keyword' : convert['meta_keyword'] if convert['meta_keyword'] else ' '}

            data_insert = self.syncFieldsInsert(data_insert, fields_table)
            products_description_query = "INSERT INTO product_description "
            products_description_query += self.arrayToInsertCondition(data_insert)
            query_key = "pd" . desc_language_id
            products_description_queries[query_key] = {'type' : 'insert',
                'query' : products_description_query}
        
     
        if(products_description_queries):
            products_description_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : products_description_queries})
            
        
        ## SEO 
        '''if(self._notice["config"]['seo_plugin'] and self._notice["config"]["seo"]){
            seo_queries = array()
            seo_name = self._notice["config"]["seo_plugin"]
            model_seo = Bootstrap.getModel(seo_name)
            data_seo = model_seo->getProductSeoExport(this, product, productsExt)
            k =1
            foreach(data_seo as seo){
                seo_query = "INSERT INTO url_alias "
                seo_query += self.arrayToInsertCondition(array(
                    'query' : 'product_id='.product_id,
                    'keyword' : seo['request_path']
                ))
                query_key = "seo" . kk++
                seo_queries[query_key] = array(
                    'type' : 'insert',
                    'query' : seo_query
                )
                break
            }
            if(seo_queries){
                product_seo_import = self.getConnectorData(url_query, array(
                    'serialize' : False,
                    'query' : serialize(seo_queries)
                ))
            }
        }'''
         

        ##related
        if(self.iset(convert,'related')):
            related_queries = {}
            for relate_id in convert['related'].items():
                relate_desc_id = self.getMapFieldBySource(self.TYPE_PRODUCT, relate_id)
                if(relate_desc_id):
                    related_query = "INSERT INTO product_related "
                    related_query += self.arrayToInsertCondition({'product_id' : product_id,
                        'related_id' : relate_desc_id})
                    related_queries [] = {'type' : 'insert',
                        'query' : related_query}

                    related_query2 = "INSERT INTO product_related "
                    related_query2 += self.arrayToInsertCondition({'product_id' : relate_desc_id,
                        'related_id' : product_id
                    })
                    related_queries [] = {'type' : 'insert',
                        'query' : related_query2}
                
            
            if(related_queries):
                products_related_import = self.getConnectorData(url_query, {'serialize' : False,
                    'query' : related_queries})
            
        
        ##end related
        ## images
        if(self.iset(convert,'images')):
            url_image = self.getConnectorUrl('image')
            images_import_data = {}
            images = {}
            for key, image in convert['images'].items():
                image_process = self.processImageBeforeImport(image['url'], image['path'])
                image_key = 'i' . key
                images_import_data[image_key] = {'type' : 'download',
                    'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_product'].'catalog/'),
                    'params' : {'url' : image_process['url'],
                        'rename' : True}}
                    
                
                images[image_key] = image
            
            if(images_import_data):
                image_import = self.getConnectorData(url_image, {'images' : images_import_data})
                if(image_import and image_import['result'] == 'success'):
                    products_images_queries = {}
                    sort_order = 0
                    for image_key, image in images.items():
                        image_import_path = (image_import['data'][image_key]) if (self.iset(image_import['data'],image_key)) else False
                        if(image_import_path):
                            image_import_path = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_product'])
                            products_images_query = "INSERT INTO product_image "
                            products_images_query += self.arrayToInsertCondition({'product_id' : product_id,
                                'image' : image_import_path,
                                'sort_order' : sort_order})
                            sort_order = sort_order + 1
                            products_images_queries[image_key] = {'type' : 'insert',
                                'query' : products_images_query}
                        
                    
                    if(products_images_queries):
                        products_images_import = self.getConnectorData(url_query, {'serialize' : False,
                            'query' : products_images_queries})
                        
                    
                
            
        
        ## categories
        if(self.iset(convert,'categories')):
            category_queries = {}
            for key, category in convert['categories'].items():
                category_id = self.getMapFieldBySource(self.TYPE_CATEGORY, category['id'], category['code'])
                if(category_id):
                    category_query = "INSERT INTO product_to_category "
                    category_query += self.arrayToInsertCondition({'product_id' : product_id,
                        'category_id' : category_id})
                    
                    query_key = "ptc" + key
                    category_queries[query_key] = {'type' : 'insert',
                        'query' : category_query}
                    
                
            
            if(category_queries)
                product_category_import = self.getConnectorData(url_query, {'serialize' : False,
                    'query' : category_queries})
                
            
        
        
        ## options
        if(self.iset(convert,'option')):

            for option in convert['options'].items():
                option_id = False
                if(option['id'] or option['code']):
                    option_id = self.getMapFieldBySource(self.TYPE_OPTION, option['id'], option['option_name'])
                
                if(option_id == False):
                    option_next_id_data = self.getConnectorData(url_query, {'query' : serialize({'type' : 'select',
                            'query' : "SELECT MAX(option_id) + 1 AS next_id FROM `option`"})})
                        
                    
                    if(option_next_id_data == False or option_next_id_data['result'] != 'success' oroption_next_id_data['data'][0]['next_id']== None):
                        option_id = 1
                    else:
                        option_id = option_next_id_data['data'][0]['next_id']
                    
                    if(option['option_type'] == 'drop_down' or option['option_type'] == 'Dropdown'):
                        type = 'select'
                    else:
                        type = option['option_type']
                    
                    option_query = "INSERT INTO `option` "
                    option_query += self.arrayToInsertCondition({'option_id' : option_id,
                        'type' : type})

                    
                    option_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                            'query' : option_query}})
                        
                    
                    
                     if(option_data == False or option_data['result'] != 'success'):
                        continue
                    
                    option_queries = {}
                    for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                        optionLang = option['option_languages'][src_language_id] if (self.iset(option['option_languages'],src_language_id)) else option
                        option_query = "INSERT INTO option_description "
                        option_query += self.arrayToInsertCondition({'option_id' : option_id,
                            'language_id' : desc_language_id,
                            'name' : optionLang['option_name']})
                        
                        query_key = 'po' + desc_language_id
                        option_queries[query_key] = {'type' : 'insert',
                            'query' : option_query}
                        
                    
                    
                    if(option_queries):
                        continue
                    
                    option_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : option_queries})
                    
                    if(option_import or option_import['result'] != 'success'):
                        continue
                    
                    if(option['id'] or option['code']):
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_OPTION, option['id'], option_id, option['option_name'])
                        self.insertdb(conn,query)
                        #self.insertMap(url_src, url_desc, self.TYPE_OPTION, option['id'], option_id, option['option_name'])
                    
                
                    
                prd_option_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                        'query' : "SELECT MAX(product_option_id) + 1 AS next_id FROM product_option"}})
                    
                
                if(prd_option_next_id_data == False or prd_option_next_id_data['result'] != 'success' or prd_option_next_id_data['data'][0]['next_id'] == None):
                    product_option_id = 1

                else:
                    product_option_id = prd_option_next_id_data['data'][0]['next_id']
                
                option_value = ''
                if(option['option_type'] in array('date','text','textarea','time','datetime')):
                    option_value = option['values'][0]['option_value_name'] if (self.iset(option['values'][0],'option_value_name')) else ''
                
                product_option_query = 'INSERT INTO product_option'
                product_option_query += self.arrayToInsertCondition({'product_option_id' : product_option_id,
                    'option_id' : option_id,
                    'product_id' : product_id,
                    'value' : option_value,
                    'required' : 1 if option['required'] else 0})
                
                
                 product_option_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                            'query' : product_option_query}})
                        
                    
                if(product_option_data == False or product_option_data['result'] != 'success'):
                   continue
                
                if(option['option_type'] in array('date','text','textarea','time','datetime')):
                    for option_value in option['values'].items():
                        option_value_id = False
                        if(option_value['id'] or option_value['code']):
                            option_value_id = self.getMapFieldBySource(self.TYPE_OPTION_VALUE, option_value['id'], option_value['code'])
                        
                        if(option_value_id == False):
                            optionValue_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                                    'query' : "SELECT MAX(option_value_id) + 1 AS next_id FROM option_value"}})
                                
                            

                            if(optionValue_next_id_data == False or optionValue_next_id_data['result'] != 'success' or optionValue_next_id_data['data'][0]['next_id'] == None):
                                option_value_id = 1
                            else:
                                option_value_id = optionValue_next_id_data['data'][0]['next_id']
                            

                            option_query = "INSERT INTO option_value "
                            option_query += self.arrayToInsertCondition({'option_value_id' : option_value_id,
                                'option_id' : option_id})
                            
                            option_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                    'query' : option_query}})
                                
                            
                            if(option_data == False or option_data['result'] != 'success'):
                                continue
                            
                            option_value_queries = {}
                            for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                                optionLang = option['option_value_languages'][src_language_id] if (self.iset(option_value['option_value_languages'],src_language_id)) else option_value
                                option_query = "INSERT INTO option_value_description "
                                option_query += self.arrayToInsertCondition({'option_value_id' : option_value_id,
                                    'option_id' : option_id,
                                    'language_id' : desc_language_id,
                                    'name' : optionLang['option_value_name']})
                                
                                query_key = 'po' . desc_language_id
                                option_value_queries[query_key] = {'type' : 'insert',
                                    'query' : option_query}
                                
                            
                            if(option_value_queries == False):
                                continue
                            
                            option_import = self.getConnectorData(url_query, {'serialize' : False,
                                'query' : option_value_queries})
                        
                            if(option_import == False or option_import['result'] != 'success'):
                                continue
                            
                            if(option_value['id'] or option_value['code']):
                                conn = self.connectdb()
                                query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_OPTION_VALUE, option_value['id'], option_value_id, option_value['option_value_name'])
                                self.insertdb(conn,query)
                                #self.insertMap(url_src, url_desc, self.TYPE_OPTION_VALUE, option_value['id'], option_value_id, option_value['option_value_name'])
                            
                        
                        product_option_value_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                                    'query' : "SELECT MAX(product_option_value_id) + 1 AS next_id FROM product_option_value"}})
                                
                            

                        if(product_option_value_next_id_data == False or product_option_value_next_id_data['result'] != 'success' or product_option_value_next_id_data['data'][0]['next_id'] == False):
                            prd_option_value_id = 1
                        else:
                            prd_option_value_id = product_option_value_next_id_data['data'][0]['next_id']
                        

                        if(prd_option_value_id):
                            fields_table = self.getAllColumnInTable('product_option_value')
                            data_insert = {'product_option_value_id' : prd_option_value_id,
                                'product_id' : product_id,
                                'option_id' : option_id,
                                'product_option_id' : product_option_id,
                                'option_value_id' : option_value_id,
                                'weight' : option_value['weight'] if (self.iset(option_value,'weight')) else 0,
                                'price' : option_value['price'] if option_value['price'] else 0 ,
                                'price_prefix' : option_value['price_prefix'] if option_value['price_prefix'] else '+'}
                            
                            data_insert = self.syncFieldsInsert(data_insert, fields_table)
                            product_attribute_join_query = "INSERT INTO product_option_value "
                            product_attribute_join_query = product_attribute_join_query + self.arrayToInsertCondition(data_insert)
                            product_option_value_queries['pov' + prd_option_value_id] = {'type' : 'insert',
                                'query' : product_attribute_join_query}
                            
                            if(product_option_value_queries == False):
                                continue
                            
                            product_option_value_import = self.getConnectorData(url_query, {'serialize' : False,
                                'query' : product_option_value_queries})
                            

                            if(product_option_value_import or product_option_value_import['result'] != 'success'):
                                
                                ##print_r(product_option_value_queries)
                                ##var_dump(product_option_value_import)
                                continue
                            
                        
                    
                
                
            
        else if(self.iset(convert,'children')):
            for option in  convert['children'].items():
                for _option in option['attributes'].items():
                    option_id = False
                    if(_option['option_id'] or _option['option_code'] ):
                        option_id = self.getMapFieldBySource(self.TYPE_OPTION, _option['option_id'], _option['option_code'])
                    
                    if(option_id == False):
                        option_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                                'query' : "SELECT MAX(option_id) + 1 AS next_id FROM `option`"}})
                            
                        
                        if(option_next_id_data or option_next_id_data['result'] != 'success' oroption_next_id_data['data'][0]['next_id']== None):
                            option_id = 1

                        else:
                            option_id = option_next_id_data['data'][0]['next_id']
                        
                        option_query = "INSERT INTO `option` "
                        option_query += self.arrayToInsertCondition({'option_id' : option_id,
                            'type' : 'select'##option['option_type'],
                        })
                        option_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                'query' : option_query}})
                            
                        

                        if(option_data or option_data['result'] != 'success'):
                            continue
                        
                        option_queries = {}
                        for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                            optionLang = _option
                            option_query = "INSERT INTO option_description "
                            option_query = option_query + self.arrayToInsertCondition({'option_id' : option_id,
                                'language_id' : desc_language_id,
                                'name' : optionLang['option_name']})
                            
                            query_key = 'po' . desc_language_id
                            option_queries[query_key] = {'type' : 'insert',
                                'query' : option_query}
                            
                        

                        if(option_queries == False):
                            continue
                        
                        option_import = self.getConnectorData(url_query, {'serialize' : False,
                            'query' : option_queries})
                        
                        if(option_import == False or option_import['result'] != 'success')
                            continue
                        
                        if(_option['option_id'] or _option['option_code']):
                            conn = self.connectdb()
                            query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_OPTION, _option['option_id'], option_id, _option['option_code'])
                            self.insertdb(conn,query)
                            #self.insertMap(url_src, url_desc, self.TYPE_OPTION, _option['option_id'], option_id, _option['option_code'])
                        
                    
                    prd_option_check = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                        'query' : "SELECT * FROM product_option WHERE option_id = '"+option_id+"' AND product_id = '"+ product_id+"' "}})

                if(prd_option_check == False or prd_option_check['result'] != 'success' or count(prd_option_check['data'] == False)):
                    prd_option_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                            'query' : "SELECT MAX(product_option_id) + 1 AS next_id FROM product_option"}})
                        
                    
                    if(prd_option_next_id_data == False or prd_option_next_id_data['result'] != 'success' or prd_option_next_id_data['data'][0]['next_id'] == None):
                        product_option_id = 1
                    else:
                        product_option_id = prd_option_next_id_data['data'][0]['next_id']
                    
                    product_option_query = 'INSERT INTO product_option'
                    product_option_query += self.arrayToInsertCondition({'product_option_id' : product_option_id,
                        'option_id' : option_id,
                        'product_id' : product_id,
                        'required' : 0}) # option['required'] ? 1 : 0})
                    

                     product_option_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                'query' : product_option_query}})
                            
                        
                    if(product_option_data == False or product_option_data['result'] != 'success'):
                       continue
                    

                else:
                    if(prd_option_check['data']):
                        product_option_id = prd_option_check['data'][0]['product_option_id']
                    
                
                option_value_id = False
                if(_option['option_value_name' == False]):
                    continue
                
                    if(_option['option_value_id'] or _option['option_value_name'] ):
                        option_value_id = self.getMapFieldBySource(self.TYPE_OPTION_VALUE, _option['option_value_id'], _option['option_value_name'])
                    
                    if(option_value_id == False):
                        optionValue_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                                'query' : "SELECT MAX(option_value_id) + 1 AS next_id FROM option_value"}})
                            
                        
                        
                        if(optionValue_next_id_data == False or optionValue_next_id_data['result'] != 'success' or optionValue_next_id_data['data'][0]['next_id'] == None):
                            option_value_id = 1
                        else:
                            option_value_id = optionValue_next_id_data['data'][0]['next_id']
                        
                    
                    option_query = "INSERT INTO option_value "
                    option_query += self.arrayToInsertCondition({'option_value_id' : option_value_id,
                        'option_id' : option_id})
                    
                    option_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                            'query' : option_query}})
                        
                    
                    if(option_data == False or option_data['result'] != 'success'):
                        continue
                    
                    option_value_queries = {}
                    for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                        optionLang = _option##isset(option['option_languages'][src_language_id]) ? option_value['option_value_languages'][src_language_id] : option_value
                        option_query = "INSERT INTO option_value_description "
                        option_query = option_query +  self.arrayToInsertCondition({'option_value_id' : option_value_id,
                            'option_id' : option_id,
                            'language_id' : desc_language_id,
                            'name' : optionLang['option_value_name']})
                        
                        query_key = 'po' + desc_language_id
                        option_value_queries[query_key] = {'type' : 'insert',
                            'query' : option_query}
                        
                    
                    if(option_value_queries == False):
                        continue
                    
                    option_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : option_value_queries})
                    
                    if(option_import == False or option_import['result'] != 'success'):
                        continue
                    
                    if(_option['option_value_id'] or_option['option_value_name'] ):
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_OPTION_VALUE, _option['option_value_id'], option_value_id, _option['option_value_name'])
                        self.insertdb(conn,query)
                        #self.insertMap(url_src, url_desc, self.TYPE_OPTION_VALUE, _option['option_value_id'], option_value_id, _option['option_value_name'])
                    
                    
                    product_option_value_check_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                            'query' : "SELECT product_option_value_id FROM product_option_value WHERE option_value_id = {option_value_id} AND product_id = {product_id}"}})
                        
                    
                    if(product_option_value_check_data['data'] and product_option_value_check_data['data'][0]['product_option_value_id']):
                        continue
                    
                    product_option_value_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select', 'query' : "SELECT MAX(product_option_value_id) + 1 AS next_id FROM product_option_value"}})
                        
                    
                          
                    if(product_option_value_next_id_data == False or product_option_value_next_id_data['result'] != 'success' or !product_option_value_next_id_data['data'][0]['next_id']):
                        prd_option_value_id = 1
                    else:
                        prd_option_value_id = product_option_value_next_id_data['data'][0]['next_id']
                    
                   
                    if(prd_option_value_id):
                        fields_table = self.getAllColumnInTable('product_option_value')
                        data_insert = {'product_option_value_id' : prd_option_value_id,
                            'product_id' : product_id,
                            'option_id' : option_id,
                            'product_option_id' : product_option_id,
                            'option_value_id' : option_value_id,
                            'weight' : _option['weight'] if (self.iset(_option,'weight')) else 0,
                            'price' :  0 if((len(option['attributes']) > 1)) else option['price'],
                            'price_prefix' : _option['price_prefix']}
                        
                        data_insert = self.syncFieldsInsert(data_insert, fields_table)
                        product_attribute_join_query = "INSERT INTO product_option_value "
                        product_attribute_join_query += self.arrayToInsertCondition(data_insert)
                        product_option_value_queries['pov' + prd_option_value_id] = {'type' : 'insert',
                            'query' : product_attribute_join_query}
                        
                       if(product_option_value_queries == False):
                            continue
                        
                        product_option_value_import = self.getConnectorData(url_query, {'serialize' : False,
                            'query' : product_option_value_queries})
                        
                        
                        if(product_option_value_import == False or product_option_value_import['result'] != 'success'):
                            #print_r(product_option_value_queries)
                            continue
                        
                    
                
                
               
            
        
        if(self.iset(convert,'attributes')):
            for attribute in convert['attributes'].items():
                attributeId = self.getMapFieldBySource(self.TYPE_ATTR, attribute['option_id'], attribute['option_code_save'])
                if(attributeId == False):
                    attributeId_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                            'query' : "SELECT MAX(attribute_id) + 1 AS next_id FROM `attribute`"}})
                        
                    
                    if(attributeId_next_id_data == False or attributeId_next_id_data['result'] != 'success' orattributeId_next_id_data['data'][0]['next_id']== None):
                        attributeId = 1
                    else:
                        attributeId = attributeId_next_id_data['data'][0]['next_id']
                    
                    if(attributeId == False):
                        continue
                    
                    attribute_query = "INSERT INTO `attribute` "
                        attribute_query += self.arrayToInsertCondition({'attribute_id' : attributeId})
                        
                        attribute_data = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                'query' : attribute_query}})
                            
                        
                    if(attribute['option_id'] orattribute['option_code_save'] ):
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_ATTR, attribute['option_id'], attributeId, attribute['option_code_save'])
                        self.insertdb(conn,query)
                        #self.insertMap(url_src, url_desc, self.TYPE_ATTR, attribute['option_id'], attributeId, attribute['option_code_save'])
                    

                    attributeLang_queries = {}
                    for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                        AttributeLang = attribute['option_languages'][src_language_id] if attribute['option_languages'][src_language_id] else attribute

                        attribute_join_query = "INSERT INTO attribute_description "
                        attribute_join_query += self.arrayToInsertCondition({'attribute_id' : attributeId,
                            'language_id' : desc_language_id,
                            'name' : AttributeLang['option_name']})
                        
                        query_key = "a_" + desc_language_id
                        attributeLang_queries[query_key] = {'type' : 'insert',
                            'query' : attribute_join_query}
                        
                    

                    if(attributeLang_queries == False):
                        continue
                    
                    attributeLang_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : attributeLang_queries})
                    
                

                for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                    attrValue = attribute['option_value_languages'][src_language_id] if (self.iset(attribute['option_value_languages'],src_language_id)) else attribute

                    attr_query = "INSERT INTO product_attribute "
                    attr_query += self.arrayToInsertCondition({'product_id' :product_id,
                        'attribute_id' : attributeId,
                        'language_id' : desc_language_id,
                        'text' : attrValue['option_value_name']})
                    
                    query_key = "pa_" + desc_language_id
                    prdattribute_queries[query_key] = {'type' : 'insert',
                        'query' : attr_query}
                    
                


                if(prdattribute_queries == False):
                       continue
                
                prdAttribute_import = self.getConnectorData(url_query, {'serialize' : False,
                    'query' : prdattribute_queries})
                

            
        
        
                           

        if(convert['special_price'] and int(convert['special_price']['price'])):
            product_special = "INSERT INTO product_special "
            product_special = product_special + self.arrayToInsertCondition({'product_id' : product_id,
                'price' : convert['special_price']['price'],
                'date_start' : convert['special_price']['start_date'] if convert['special_price']['start_date'] else '0000-00-00',
                'date_end' : convert['special_price']['end_date'] if convert['special_price']['end_date'] else '0000-00-00',
                'customer_group_id' : 0})
        

            product_special_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                    'query' : product_special}})
                
            

            if(product_special_import == False or product_special_import['result'] != 'success'):
                ## warning
                return self.errorConnector()
            
        
        
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    

    def additionProductImport(self,product_id, convert, product, productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    


   # CUSTOMER

   def prepareCustomersImport(self):
        customer_alter = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'query',
                'query' : "ALTER TABLE customer MODIFY COLUMN password varchar(255)"}})
            
        
        if(customer_alter == False or customer_alter['result'] != 'success'):
            return self.errorConnector()
        
       
        return self
    

    def getCustomersMainExport(self):
        last_id_f = self.get_id_last('Customer')
        #id_src = self._notice['process']['customers']['id_src']
        #limit = self._notice['setting']['customers']
        customers = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM customer WHERE customer_id > " + last_id_f + " ORDER BY customer_id ASC LIMIT " + last_id_f}})
            
        
        if(customers == False or customers['result'] != 'success'):
            return self.errorConnector()
        
        return customers
    

    def getCustomersExtExport(self,customers):
        query_url = self.getConnectorUrl('query')
        customerId = self.duplicateFieldValueFromList(customers['data'], 'customer_id')
        customer_id_con = self.arrayToInCondition(customerId)
        customer_ext_queries = {'customers_address' : {'type' : 'select',
                'query' : "SELECT * FROM address WHERE customer_id IN " + customer_id_con}}
            
        
        ## add custom
        customersExt = self.getConnectorData(query_url, {'serialize' : False,
            'query' : customer_ext_queries})

        if(customersExt == False or customersExt['result'] != 'success'):
            return self.errorConnector()
        
        countryId = self.duplicateFieldValueFromList(customersExt['data']['customers_address'], 'country_id')
        stateId = self.duplicateFieldValueFromList(customersExt['data']['customers_address'], 'zone_id')
        country_id_con = self.arrayToInCondition(countryId)
        state_id_con = self.arrayToInCondition(stateId)
        customer_ext_rel_queries = {'countries' : {'type' : 'select',
                'query' : "SELECT * FROM country WHERE country_id IN " + country_id_con},
            'zones' : {'type' : 'select',
                'query' : "SELECT * FROM zone WHERE zone_id IN " + state_id_con}}
            
        
        ## add custom
        customersExtRel = self.getConnectorData(query_url, {'serialize' : False,
            'query' : customer_ext_rel_queries})
        
        if(customersExtRel == False or customersExtRel['result'] != 'success'):
            return self.errorConnector()
        
        customersExt = self.syncConnectorObject(customersExt, customersExtRel)
        return customersExt
    

    def convertCustomerExport(self,customer, customersExt):
    
        customer_data = self.constructCustomer()
        customer_data['id'] = customer['customer_id']
        customer_data['customer_group_id'] = customer['customer_group_id']
        customer_data['username'] = customer['email']
        customer_data['email'] = customer['email']
        customer_data['password'] = customer['password']
        customer_data['first_name'] = customer['firstname']
        customer_data['last_name'] = customer['lastname']
        customer_data['is_subscribed'] = customer['newsletter']
        customer_data['active'] = customer['status'] ? True : False
        customer_data['created_at'] = customer['date_added'] ? customer['date_added'] : '0000-00-00 00:00:00'
        #customer_data['code'] = customer['code']
        addressBook = self.getListFromListByField(customersExt['data']['customers_address'], 'customer_id', customer['customer_id'])
        if(addressBook):
            for address_book in addressBook.items():
                address_data = self.constructCustomerAddress()
                address_data['id'] = address_book['address_id']
                address_data['first_name'] = address_book['firstname']
                address_data['last_name'] = address_book['lastname']
                #address_data['gender'] = address_book['entry_gender']
                address_data['address_1'] = address_book['address_1']
                address_data['address_2'] = address_book['address_2']
                address_data['city'] = address_book['city']
                address_data['postcode'] = address_book['postcode']
                address_data['telephone'] = customer['telephone']
                address_data['company'] = address_book['company']
                address_data['fax'] = customer['fax']

                country = self.getRowFromListByField(customersExt['data']['countries'], 'country_id', address_book['country_id'])
                
                if(country):
                    address_data['country']['id'] = country['country_id']
                    address_data['country']['country_code'] = country['iso_code_2']
                    address_data['country']['name'] = country['name']
                else:
                    address_data['country']['country_code'] = 'US'
                    address_data['country']['name'] = 'United States'
                

                state_id = address_book['zone_id']
                if(state_id):
                    state = self.getRowFromListByField(customersExt['data']['zones'], 'zone_id', state_id)
                    if(state):
                        address_data['state']['id'] = state['zone_id']
                        address_data['state']['state_code'] = state['code']
                        address_data['state']['name'] = state['name']
                    else: 
                        address_data['state']['state_code'] = 'AL'
                        address_data['state']['name'] = 'Alabama'
                    
                else:
                    address_data['state']['name'] = ''##address_book['entry_state']
                
                if(address_book['address_id'] == customer['address_id']):
                    address_data['default']['billing'] = True
                    address_data['default']['shipping'] = True
                
                customer_data['address'][] = address_data
            
        

        return {'result' : 'success',
            'msg' : '',
            'data' : customer_data}
    

    def getCustomerIdImport(self,convert, customer, customersExt):
        return convert['id']
    
    

    def routerCustomerImport(self,convert, customer, customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
    

    def beforeCustomerImport(self,convert, customer, customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    

    def customerImport(self,convert, customer, customersExt):
        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')

        if(self.iset(convert,'fax')):
            fax = convert['fax']
        else:
            fax = ''
        
        telephone = ''
        if(self.iset(convert,'telephone')):
            telephone = convert['telephone']
        else if(convert['address']):
            for key, address in convert['address'].items():
                if(self.iset(address,'telephone')):
                    telephone = address['telephone']
                    break
                
            
        

        fields_table = self.getAllColumnInTable('customer')        
       
        if(self.iset(convert,'customer_group_id') and self.iset(self._notice["map"]["customer_group"],convert['customer_group_id'])):
            customer_group_id = self._notice["map"]["customer_group"][convert['customer_group_id']]
        else:
            customer_group_id = 1
        
        
        data_insert = {'customer_group_id' : customer_group_id,
            'firstname' : convert['first_name'] if convert['first_name'] else ' ',
            'lastname' : convert['middle_name'] . ' ' . convert['last_name'] if convert['middle_name'] else (convert['last_name'] if  convert['last_name'] else ''),
            'approved' : 1,
            'fax' : fax,
            'telephone' : telephone,
            'email' : convert['email'],
            'password' : convert['password'],
            'newsletter' : 1 if convert['is_subscribed'] else 0,
            'status' : 1 if convert['active'] else 0,
            'date_added' : self.convertStringToDatetime(convert['created_at']) if convert['created_at'] else self.datetimeNow("Y-m-d h:i:s"),
            'safe' : 1,
            'salt' : convert['salt'] if (self.iset(convert,'salt')) else ''}
        
        data_insert = self.syncFieldsInsert(data_insert, fields_table)
        if(self._notice['config']['pre_cus']):
            delete_customer = self.deleteTargetCustomer(convert['id'])
            customer_data['customer_id'] = convert['id']
        
        
        customer_query = "INSERT INTO customer "
        customer_query += self.arrayToInsertCondition(data_insert)
        ##var_dump(customer_query)exit()


        customer_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : customer_query,
                'params' : {'insert_id' : True}}})
        if(customer_import == False or customer_import['result'] != 'success' or customer_import['data'] == False):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        customer_id = customer_import['data']
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self::TYPE_CUSTOMER, convert['id'], customer_id, convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_desc, self::TYPE_CUSTOMER, convert['id'], customer_id, convert['code'])
        return {'result' : 'success',
            'msg' : '',
            'data' : customer_id}
    




    def afterCustomerImport(self,customer_id, convert, customer, customersExt):
        url_src = self_notice['src']['cart_url']
        url_desc = self_notice['target']['cart_url']
        url_query = selfgetConnectorUrl('query')
        queries = {}

        # customer rewards
        if(self.iset(convert,'rewads_points')):
            
            customer_reward_queries = {}
            for key, reward in convert['rewads_points'].items():
                if((reward['rewardpoints_status'] == 'complete' and strtotime(reward['date_end']) > strtotime('now') ) or reward['order_id'] == '-3' or !reward['date_end'] ):
                    customer_reward_query = "INSERT INTO customer_reward "
                    customer_reward_query = customer_reward_query + selfarrayToInsertCondition({'customer_id' : reward['customer_id'],
                        'order_id' : reward['order_id'],
                        'description' : reward['rewardpoints_description'] if reward['rewardpoints_description'] else '',
                        'points' : reward['points_current'] - reward['points_spent'],
                        'date_added' : reward['date_insertion'] if reward['date_insertion'] else '0000-00-00 00:00:00'})
                    query_key = "crw" + key
                    customer_reward_queries[query_key] = {'type' : 'insert',
                        'query' : customer_reward_query}
                    
                
            
            if(customer_reward_queries):
                customer_reward_import = selfgetConnectorData(url_query, {'serialize' : False,
                    'query' : customer_reward_queries})
                
            
    
        
        ## address
        address_default_key  = False
        k = 1
        for key, address in convert['address'].items():
            address_id = False
            if(address['id'] or address['code']):
                address_id = selfgetMapFieldBySource(self.TYPE_ADDRESS, address['id'], address['code'])
            
            if(address_id):
                if(address['default']['billing'])
                    customer_update_query = "UPDATE customer SET "
                    customer_update_query = customer_update_query + selfarrayToSetCondition({'address_id' : address_id,
                        'telephone' : address['telephone'],
                        'fax' : address['fax']})

                    customer_update_query = customer_update_query + " WHERE "
                    customer_update_query = customer_update_query + selfarrayToWhereCondition({'customer_id' : customer_id})
                    
                    queries['c'] = {'type' : 'query',
                        'query' : customer_update_query}
                    
                
                continue
            

            country_state_detected_queries = {}
            country_query = "SELECT country_id FROM country WHERE "
            if(address['country']['country_code']):
                country_query = country_query + " iso_code_2 = " + selfescape(address['country']['country_code'])
            else:
                country_query = country_query + " name = " + selfescape(address['country']['name'])
            
            country_state_detected_queries['c'] = {'type' : 'select',
                'query' : country_query}
            

            state_query = "SELECT zone_id FROM zone WHERE country_id = (" + country_query + ")"
            if(address['state']['state_code'])
                state_query = state_query + " AND code = " + selfescape(address['state']['state_code'])
            else:
                state_query = state_query + " AND name = " + selfescape(address['state']['name'])
            
            country_state_detected_queries['z'] = {'type' : 'select',
                'query' : state_query}
            
            country_state_detected = selfgetConnectorData(url_query, {'serialize' : False,
                'query' : country_state_detected_queries})
            
            if(country_state_detected == False or country_state_detected['result'] != 'success'):
                country_id = 1
                state_id = False
            else:
                country_id = country_state_detected['data']['c'][0]['country_id'] if (self.iset(country_state_detected['data']['c'][0],'country_id')) else 1
                state_id = country_state_detected['data']['z'][0]['zone_id'] if (country_state_detected['data']['z'][0]['zone_id']) else False
            
            fields_table = selfgetAllColumnInTable('address')
            data_insert = {'customer_id' : customer_id,
                'company' : address['company'] if  address['company'] else '',
                'firstname' : address['first_name'] if  address['first_name'] else '',
                'lastname' : address['middle_name'] + ' ' + address['last_name'] if address['middle_name'] else address['last_name'],
                'address_1' : address['address_1'] if  address['address_1'] else '',
                'address_2' : address['address_2'] if  address['address_2'] else '',
                'postcode' : address['postcode'] if address['postcode'] else '',
                'city' : address['city'] if address['city'] else '',
                'country_id' : country_id,
                'zone_id' : state_id if state_id else 0}
            
            data_insert = selfsyncFieldsInsert(data_insert, fields_table)
            
            address_query = "INSERT INTO address "
            address_query = address_query + selfarrayToInsertCondition(data_insert)
            queries['address_' + address['id']+'_'+address['code']+'_'+k] = {'type' : 'insert',
                'query' : address_query,
                'params' : {'insert_id' : True}}
                
            
            if(address['default']['billing'])
                address_default_key = 'address_' + address['id']+'_'+address['code']+'_'+k
            
        

        if(queries):
            imports = selfgetConnectorData(url_query, {'serialize' : False,
                'query' : queries})
            
            if(imports and imports['result'] == 'success'):
                for  address_id, id_desc in imports['data'].items():
                    _add = address_id.strip('_')
                    src_id = _add[1]
                    if((int)src_id and id_desc):
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc) values('%s','%s','%s',%d,%d)""" %(url_src, url_desc, self.TYPE_ADDRESS, src_id, id_desc)
                        self.insertdb(conn,query)
                        #self.insertMap(url_src, url_desc, self.TYPE_ADDRESS, src_id, id_desc)
                    
                    if(address_default_key == address_id):
                        address_default_id = imports['data'][address_default_key] if(self.iset(imports['data'],address_default_key))  else False
                        if(address_default_id):
                            address_default_update_query = "UPDATE customer SET "
                            address_default_update_query = address_default_update_query + selfarrayToSetCondition({'address_id' : address_default_id})
                            address_default_update_query = address_default_update_query + " WHERE "
                            address_default_update_query = address_default_update_query + selfarrayToWhereCondition({'customer_id' : customer_id})
                            address_default_update = selfgetConnectorData(url_query, {'query' : {'type' : 'query',
                                    'query' : address_default_update_query,}})
                                
                            
                    
                    
                
                
            
        
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        


    def additionCustomerImport(self,customer_id, convert, customer, customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
        



    def prepareOrdersImport(self):
        url_query = self.getConnectorUrl('query')
        data = self.getConnectorData(url_query, {'serialize' : False,
            'query' : {'currencies' : {'type' : 'select',
                    'query' : "SELECT * FROM currency"}}})
                
            
        
        if(data == False or data['result'] != 'success'):
            return self
        
        currencies = data['data']['currencies']
        if(currencies == False):
            currencies = self.oscDefaultCurrency()
        
        self._notice['target']['extends']['currencies'] = currencies
        return self
    


    def getOrdersMainExport(self):
        last_id_f = self.get_id_last(Order)
        #id_src = self._notice['process']['orders']['id_src']
        #limit = self._notice['setting']['orders']
        orders = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM `order` WHERE order_id > " + last_id_f + " ORDER BY order_id ASC LIMIT " + last_id_f}})
            
        
        if(orders == False or orders['result'] != 'success'):
            return self.errorConnector()
        
        return orders
    
    





    def getOrdersExtExport(self,orders):
        url_query = self.getConnectorUrl('query')
        orderId = self.duplicateFieldValueFromList(orders['data'], 'order_id')
        currencyCode = self.duplicateFieldValueFromList(orders['data'], 'currency_id')
        payCountry = {self.duplicateFieldValueFromList(orders['data'], 'payment_country')}
        shippingCountry = {self.duplicateFieldValueFromList(orders['data'], 'shipping_country')}
        countries = payCountry + shippingCountry
        payState = {self.duplicateFieldValueFromList(orders['data'], 'payment_zone')}
        shipState = {self.duplicateFieldValueFromList(orders['data'], 'shipping_zone')}
        cusState = {self.duplicateFieldValueFromList(orders['data'], 'customers_state')}
        states = payState + shipState
        
        order_id_con = self.arrayToInCondition(orderId)
        currency_con = self.arrayToInCondition(currencyCode)
        countries_con = self.arrayToInCondition(countries, False)
        states_con = self.arrayToInCondition(states, False)
        orders_ext_queries = {'orders_total' : {'type' : 'select',
                'query' : "SELECT * FROM `total` WHERE order_id IN " + order_id_con},
            'orders_products' : {'type' : 'select',
                'query' : "SELECT * FROM order_product WHERE order_id IN " + order_id_con},
            'order_option' : {'type' : 'select',
                'query' : "SELECT * FROM order_option WHERE order_id IN " + order_id_con},
            'order_status' : {'type' : 'select',
                'query' : "SELECT * FROM order_status"},
            'order_history' : {'type' : 'select',
                'query' : "SELECT *  FROM order_history WHERE order_id IN '%s' ORDER BY order_history_id DESC" %(order_id_con)},
            'order_voucher' : {'type' : 'select',
                'query' : "SELECT * FROM order_voucher WHERE order_id IN " + order_id_con},
             'order_custom_field' : {'type' : 'select',
                'query' : "SELECT * FROM order_custom_field WHERE order_id IN " + order_id_con},
            'order_recurring' : {'type' : 'select',
                'query' : "SELECT * FROM order_recurring WHERE order_id IN " + order_id_con},
            'currencies' : {'type' : 'select',
                'query' : "SELECT * FROM currency WHERE code IN " + currency_con},
            'countries' : {'type' : 'select',
                'query' : "SELECT * FROM country WHERE name IN " + countries_con},
            'zones' : {'type' : 'select',
                'query' : "SELECT * FROM zone WHERE zone_id IN " + states_con}}
        
        # add custom
        ordersExt = self.getConnectorData(url_query, {'serialize' : False,
            'query' : orders_ext_queries})
        if(ordersExt == False or ordersExt['result'] != 'success'):
            return self.errorConnector()
        
        product_option_value_ids = self.duplicateFieldValueFromList(ordersExt['data']['order_option'], 'product_option_value_id')
        product_option_value_id_con = self.arrayToInCondition(product_option_value_ids)
        order_recurrings = self.duplicateFieldValueFromList(ordersExt['data']['order_recurring'], 'order_recurring_id')
        order_recurring_con = self.arrayToInCondition(order_recurrings)
        orders_ext_rel_queries = {'order_recurring_transaction' : {'type' : 'select',
                'query' : "SELECT * FROM order_recurring_transaction WHERE order_recurring_id IN "+ order_recurring_con},
            'orders_product_options_value' : {'type' : 'select',
                'query' : "SELECT * FROM product_option_value WHERE product_option_value_id IN "+ product_option_value_id_con}}
        # add custom
        if(orders_ext_rel_queries):
            ordersExtRel = self.getConnectorData(url_query, {'serialize' : False,
                'query' : orders_ext_rel_queries})
            
            if(ordersExtRel == False or ordersExtRel['result'] != 'success'):
                return self.errorConnector()
            
            ordersExt = self.syncConnectorObject(ordersExt, ordersExtRel)
        
        return ordersExt
    

    def convertOrderExport(self,order, ordersExt):
        order_data = self.constructOrder()
        order_data['id'] = order['order_id']
      
        order_data['status'] = order['order_status_id']
        
        orderTotal = self.getListFromListByField(ordersExt['data']['orders_total'], 'order_id', order['order_id'])
        otTax = self.getRowFromListByField(orderTotal, 'code', 'tax')
        if(otTax):
            order_data['tax']['title'] = otTax['title']
            order_data['tax']['amount'] = otTax['value']
        
        otShipping = self.getRowFromListByField(orderTotal, 'code', 'shipping')
        if(otShipping):
            order_data['shipping']['title'] = otShipping['title']
            order_data['shipping']['amount'] = otShipping['value']
        
        otSubtotal = self.getRowFromListByField(orderTotal, 'code', 'sub_total')
        if(otSubtotal):
            order_data['subtotal']['title'] = otSubtotal['title']
            order_data['subtotal']['amount'] = otSubtotal['value']
        
        otTotal = self.getRowFromListByField(orderTotal, 'code', 'total')
        if(otTotal):
            order_data['total']['title'] = otTotal['title']
            order_data['total']['amount'] = otTotal['value']
        

        order_data['currency'] = order['currency_id']

        order_data['created_at'] = order['date_added'] ? order['date_added'] : '0000-00-00 00:00:00'
        order_data['updated_at'] = order['date_modified']? order['date_modified'] : '0000-00-00 00:00:00'

        order_customer = self.constructOrderCustomer()
        order_customer['id'] = order['customer_id']
        order_customer['email'] = order['email']
        order_customer['first_name'] = order['firstname']
        order_customer['last_name'] = order['lastname']
        order_data['customer'] = order_customer

        customer_address = self.constructOrderAddress()
        customer_address['first_name'] = order['firstname']
        customer_address['last_name'] = order['lastname']
        customer_address['address_1'] = order['payment_address_1']
        customer_address['address_2'] = order['payment_address_2']
        customer_address['city'] = order['payment_city']
        customer_address['postcode'] = order['payment_postcode']
        customer_address['telephone'] = order['telephone']
        customer_address['company'] = order['payment_company']
        customer_country = self.getRowFromListByField(ordersExt['data']['countries'], 'name', order['payment_country'])
        if(customer_country):
            customer_address['country']['id'] = customer_country['country_id']
            customer_address['country']['country_code'] = customer_country['iso_code_2']
        
        customer_address['country']['name'] = order['payment_country']
        customer_state = self.getRowFromListByField(ordersExt['data']['zones'], 'name', order['payment_zone'])
        if(customer_state):
            customer_address['state']['id'] = customer_state['zone_id']
            customer_address['state']['state_code'] = customer_state['code']
        
        customer_address['state']['name'] = order['payment_zone']
        order_data['customer_address'] = customer_address

        order_billing = self.constructOrderAddress()
  
        order_billing['first_name'] = order['firstname']
        order_billing['last_name'] = order['lastname']
        order_billing['address_1'] = order['payment_address_1']
        order_billing['address_2'] = order['payment_address_2']
        order_billing['city'] = order['payment_city']
        order_billing['postcode'] = order['payment_postcode']
        order_billing['telephone'] = order['telephone']
        order_billing['company'] = order['payment_company']
        billing_country = self.getRowFromListByField(ordersExt['data']['countries'], 'name', order['payment_country'])
        if(billing_country):
            order_billing['country']['id'] = billing_country['country_id']
            order_billing['country']['country_code'] = billing_country['iso_code_2']
        
        order_billing['country']['name'] = order['payment_country']
        billing_state = self.getRowFromListByField(ordersExt['data']['zones'], 'name', order['payment_zone'])
        if(billing_state):
            order_billing['state']['id'] = billing_state['zone_id']
            order_billing['state']['state_code'] = billing_state['code']
        
        order_billing['state']['name'] = order['payment_zone']
        order_data['billing_address'] = order_billing

        order_delivery = self.constructOrderAddress()
        
        order_delivery['first_name'] = order['shipping_firstname']
        order_delivery['last_name'] = order['shipping_lastname']
        order_delivery['address_1'] = order['shipping_address_1']
        order_delivery['address_2'] = order['shipping_address_2']
        order_delivery['city'] = order['shipping_city']
        order_delivery['postcode'] = order['shipping_postcode']
        order_delivery['telephone'] = order['telephone']
        order_delivery['company'] = order['shipping_company']
        delivery_country = self.getRowFromListByField(ordersExt['data']['countries'], 'name', order['shipping_country'])
        if(delivery_country):
            order_delivery['country']['id'] = delivery_country['country_id']
            order_delivery['country']['country_code'] = delivery_country['iso_code_2']
        
        order_delivery['country']['name'] = order['shipping_country']
        delivery_state = self.getRowFromListByField(ordersExt['data']['zones'], 'name', order['shipping_zone'])
        if(delivery_state):
            order_delivery['state']['id'] = delivery_state['zone_id']
            order_delivery['state']['state_code'] = delivery_state['code']
        
        order_delivery['state']['name'] = order['shipping_zone']
        order_data['shipping_address'] = order_delivery

        order_payment = self.constructOrderPayment()
        order_payment['title'] = order['payment_method']
        order_data['payment'] = order_payment

        orderProduct = self.getListFromListByField(ordersExt['data']['orders_products'], 'order_id', order['order_id'])
        orderProductOptions = self.getListFromListByField(ordersExt['data']['order_option'], 'order_id', order['order_id'])
        orderItem = {}
        for  order_product in orderProduct.items():
            order_item_subtotal = order_product['price'] * order_product['quantity']
            order_item_tax = order_item_subtotal * order_product['tax']
            order_item_total = order_item_subtotal + order_item_tax
            order_item = self.constructOrderItem()
            order_item['id'] = order_product['order_product_id']
            order_item['product']['id'] = order_product['product_id']
            order_item['product']['name'] = order_product['name']
            order_item['product']['sku'] = order_product['model']
            order_item['qty'] = order_product['quantity']
            order_item['price'] = order_product['price']
            order_item['original_price'] = order_product['price']
            order_item['tax_amount'] = order_item_tax
            order_item['tax_percent'] = order_product['tax']
            order_item['discount_amount'] = '0.0000'
            order_item['discount_percent'] = '0.0000'
            order_item['subtotal'] = order_item_subtotal
            order_item['total'] = order_item_total
            orderProductOption = self.getListFromListByField(orderProductOptions, 'order_product_id', order_product['order_product_id'])
            if(orderProductOption):
                orderItemOption = {}
                for order_product_option in orderProductOption.items():
                    order_item_option = self.constructOrderItemOption()
                    order_item_option['option_name'] = order_product_option['name']
                    order_item_option['option_value_name'] = order_product_option['value']
                    order_product_option_value = self.getRowFromListByField(ordersExt['data']['orders_product_options_value'], 'product_option_value_id', order_product_option['product_option_value_id'])
                    if(order_product_option_value):
                        order_item_option['price'] = order_product_option_value['price']
                        order_item_option['price_prefix'] = order_product_option_value['price_prefix']
                    else:
                        order_item_option['price'] = 0
                        order_item_option['price_prefix'] = ''
                    
                    
                    orderItemOption[] = order_item_option
                
                order_item['options'] = orderItemOption
            
            orderItem[] = order_item
        
        order_data['items'] = orderItem
        
        orderHistory = {}
        histories = self.getListFromListByField(ordersExt['data']['order_history'], 'order_id', order['order_id'])
        if(histories):
            orderHistories = {}
            for history in histories.items():
                order_history = self.constructOrderHistory()
                order_history['created_at'] = history['date_added']
                order_history['notify'] = history['notify']
                order_history['status'] = history['order_status_id']
                order_history['comment'] = history['comment']
                orderHistories[] = order_history
            
            order_data['histories'] = orderHistories
        
        return {'result' : 'success',
            'msg' : '',
            'data' : order_data}
    

    def getOrderIdImport(self,convert, order, ordersExt):
        return convert['id']

    def checkOrderImport(convert, order, ordersExt):
        return True if self.getMapFieldBySource(self.TYPE_ORDER, convert['id'], convert['code']) else  False

    def routerOrderImport(self,convert, order, ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'orderImport'}
        
    

    def beforeOrderImport(self,convert, order, ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
    

    def orderImport(self,convert, order, ordersExt):
        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        
        customer = convert['customer']
        customer_address = convert['customer_address']
        billing_address = convert['billing_address']
        shipping_address = convert['shipping_address']
        customer_id = self.getMapFieldBySource(self.TYPE_CUSTOMER, convert['customer']['id'], convert['customer']['code'])
        custom_field = ''
        if(customer_id == False):
            customer_id = 0
        
        customer_group_id = 0
        if(self.iset(convert['customer'],'customer_group_id')):
            customer_group_id = self._notice["map"]["customer_group"][convert['customer']['customer_group_id']]
        else if(customer_id):
            customers = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                    'query' : "SELECT * FROM `customer` WHERE customer_id = "+ customer_id}})
                
            
            if(customers == False or customers['result'] != 'success'):
                return self.errorConnector()
            
            if(customers['data']):
                customer_group_id = self.getRowValueFromListByField(customers['data'], 'customer_id', customer_id, 'customer_group_id')
        
        
        order_status = self._notice['map']['order_status'][convert['status']] if (self.iset(self._notice['map']['order_status'],convert['status']))   else 1
        currency_id =  self._notice['map']['currencies'][convert['currency']] if( self.iset(self._notice['map']['currencies'],convert['currency'])) else 2
             
        currency = self.getRowFromListByField(self._notice['target']['extends']['currencies'], 'currency_id', currency_id)
       
        if(currency):
           
            currency_code = currency['code']
            currency_value = '1.000000'#currency['value']
        else:
            currency_code = 'USD'
            currency_value = '1.000000'
        
        total = convert['total']['amount']
        fields_table = self.getAllColumnInTable('order')
        data_insert = {'customer_id' : customer_id,
            'customer_group_id' : customer_group_id,
            'payment_firstname' : billing_address['first_name']if billing_address['first_name'] else '',
            'payment_lastname' : billing_address['last_name']if billing_address['last_name'] else '',
            'payment_company' : billing_address['company']if billing_address['company'] else '',
            'payment_address_1' : billing_address['address_1'] if billing_address['address_1'] else '',
            'payment_address_2' : billing_address['address_2'] if billing_address['address_2'] else '',
            'payment_city' : billing_address['city'] if billing_address['city'] else '',
            'payment_postcode' : billing_address['postcode'] if billing_address['postcode'] else '',
            'payment_zone' : billing_address['state']['name'] if billing_address['state']['name'] else '',
            'payment_country' : billing_address['country']['name'] if billing_address['country']['name'] else '',
            'payment_country_id' : self.getCountryId(billing_address['country']['country_code'],billing_address['country']['name']),
            'payment_zone_id' : self.getStateId(billing_address['state']['state_code'],billing_address['state']['name']),
            'telephone' : billing_address['telephone'] if billing_address['telephone'] else '',
            'email' : customer['email'] if customer['email'] else '',
            'firstname' : customer['first_name'] if customer['first_name'] else '',
            'lastname' : customer['last_name'] if customer['last_name'] else '',
            
            'shipping_method' : convert['shipping']['title'] if convert['shipping']['title'] else '',
            'shipping_firstname' : shipping_address['first_name'] if shipping_address['first_name'] else '',
            'shipping_lastname' : shipping_address['last_name'] if shipping_address['last_name'] else '',
            'shipping_company' : shipping_address['company'] if shipping_address['company'] else '',
            'shipping_address_1' : shipping_address['address_1'] if shipping_address['address_1'] else '',
            'shipping_address_2' : shipping_address['address_2'] if shipping_address['address_2'] else '',
            'shipping_city' : shipping_address['city'] if shipping_address['city'] else '',
            'shipping_postcode' : shipping_address['postcode'] if shipping_address['postcode'] else '',
            'shipping_zone' : shipping_address['state']['name'] if shipping_address['state']['name'] else '',
            'shipping_country' : shipping_address['country']['name'] if shipping_address['country']['name'] else '',
            'shipping_country_id' : self.getCountryId(shipping_address['country']['country_code'],shipping_address['country']['name']),
            'shipping_zone_id' : self.getStateId(shipping_address['state']['state_code'],shipping_address['state']['name']),
            'payment_method' : convert['payment']['title'] if convert['payment']['title'] else '',
            'date_modified' : self.convertStringToDatetime(convert['updated_at']) if convert['updated_at'] else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total' : total,
            'date_added' : convert['created_at'] ? self.convertStringToDatetime(convert['created_at']) else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'order_status_id' : order_status,
            'currency_code' : currency_code,
            'currency_id' : currency_id,
            'currency_value' : currency_value}

        data_insert = self.syncFieldsInsert(data_insert, fields_table)
        if(self._notice['config']['pre_ord']):
            order_delete = self.deleteTargetOrder(convert['id'])
            data_insert['order_id'] = convert['id']
        
        order_query = "INSERT INTO `order` "
        order_query += self.arrayToInsertCondition(data_insert)
        
        order_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : order_query,
                'params' : {'insert_id' : True}}})
                
            
        

        if(order_import == False or order_import['result'] != 'success' or order_import['data'] == False):
            #var_dump(order_query)exit
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        order_id = order_import['data']

        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_ORDER, convert['id'], order_id, convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_desc, self.TYPE_ORDER, convert['id'], order_id, convert['code'])

        return {'result' : 'success',
            'msg' : '',
            'data' : order_id}
    

    def afterOrderImport(self,order_id, convert, order, ordersExt):
        url_query = self.getConnectorUrl('query')

        # order total
        orders_total_queries = {}
        currency_id = self._notice['map']['currencies'][convert['currency']] if (self.iset(self._notice['map']['currencies'],convert['currency']))   else 1
        currency = self.getRowFromListByField(self._notice['target']['extends']['currencies'], 'currency_id', currency_id)
        ot_sort_order = 1
        otTax = convert['tax']
        if(otTax['amount']):
            ot_tax_title = self.oscOrderTitleFormat(otTax['title']) if otTax['title']  else 'Tax:'
            ot_tax_value = otTax['amount']
            
            ot_tax_query = "INSERT INTO order_total "
            ot_tax_query = ot_tax_query + self.arrayToInsertCondition({'order_id' : order_id,
                'title' : ot_tax_title,
                'value' : ot_tax_value,
                'code' : 'tax',
                'sort_order' : ot_sort_order})
            ot_sort_order = ot_sort_order + 1 
            orders_total_queries['tax'] = {'type' : 'insert',
                'query' : ot_tax_query}


        otShipping = convert['shipping']
        if(otShipping['amount']):
            ot_shipping_title = self.oscOrderTitleFormat(otShipping['title']) if otShipping['title']  else 'Shipping:'
            ot_shipping_value = otShipping['amount']
            ot_shipping_query = "INSERT INTO order_total "
            ot_shipping_query = ot_shipping_query + self.arrayToInsertCondition({'order_id' : order_id,
                'title' : ot_shipping_title,
                'value' : ot_shipping_value,
                'code' : 'shipping',
                'sort_order' : ot_sort_order})
            ot_sort_order = ot_sort_order + 1
            orders_total_queries['shipping'] = {'type' : 'insert',
                'query' : ot_shipping_query}
        

        otSubtotal = convert['subtotal']
        if(otSubtotal['amount']):
            ot_subtotal_title = self.oscOrderTitleFormat(otSubtotal['title']) if otSubtotal['title'] else 'Sub-Total:'
            ot_subtotal_value = otSubtotal['amount']
            ot_subtotal_query = "INSERT INTO order_total "
            ot_subtotal_query += self.arrayToInsertCondition({'order_id' : order_id,
                'title' : ot_subtotal_title,
                'value' : ot_subtotal_value,
                'code' : 'subtotal',
                'sort_order' : ot_sort_order})
            ot_sort_order = ot_sort_order + 1
            orders_total_queries['subtotal'] = {'type' : 'insert',
                'query' : ot_subtotal_query}
        

        otTotal = convert['total']
        if(otTotal['amount']):
            ot_total_title = self.oscOrderTitleFormat(otTotal['title']) if otTotal['title'] else 'Total:'
            ot_total_value = otTotal['amount']
            order_update_query = "UPDATE `order` SET "
            order_update_query += self.arrayToSetCondition({'total' : ot_total_value})
            order_update_query += " WHERE "
            order_update_query += self.arrayToWhereCondition({'order_id' : order_id})
            order_update = self.getConnectorData(url_query, {'query' : {'type' : 'query',
                    'query' : order_update_query}})
            
            ot_total_query = "INSERT INTO order_total "
            ot_total_query += self.arrayToInsertCondition({'order_id' : order_id,
                'title' : ot_total_title,
                'value' : ot_total_value,
                'code' : 'total',
                'sort_order' : ot_sort_order})
            orders_total_queries['total'] = {'type' : 'insert',
                'query' : ot_total_query}
        
        if(orders_total_queries):
            orders_total_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : orders_total_queries})
        

        # order product
        items = convert['items']
        order_product_queries = {}
        order_product_options = {}
        for  key, item in items.items():
            product_id = self.getMapFieldBySource(self.TYPE_PRODUCT, item['product']['id'], item['product']['code'])
            if(product_id == False):
                product_id = 0
            
            fields_table = self.getAllColumnInTable('order_product')
            data_insert = {'order_id' : order_id,
                'product_id' : product_id,
                'model' : item['product']['sku'],
                'name' : item['product']['name'],
                'price' : item['price'],
                'tax' : item['tax_percent'],
                'quantity' : item['qty'],
                'total' : item['total']}
        
            data_insert = self.syncFieldsInsert(data_insert, fields_table)
            item_query = "INSERT INTO order_product "
            item_query += self.arrayToInsertCondition(data_insert)
            query_key = 'op' + key
            order_product_queries[query_key] = {'type' : 'insert',
                'query' : item_query,
                'params' : {'insert_id' : True}}
            
            
            if(item['options']):
                for  option in ['options'].items():
                    product_option_id = self.getMapFieldBySource(self.TYPE_OPTION, option['option_id'])
                    product_option_value_id = self.getMapFieldBySource(self.TYPE_OPTION, option['option_value_id'])
                    option_data = {'order_id' : order_id,
                        'order_product_id' : query_key,
                        'product_option_id' : product_option_id ? product_option_id : 0,
                        'product_option_value_id' : product_option_value_id ? product_option_value_id : 0,
                        'name' : option['option_name'],
                        'value' : option['option_value_name']}
                    order_product_options[] = option_data
                
            
        
        if(order_product_queries):
            order_product_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : order_product_queries})
            if(order_product_import and order_product_import['result'] == 'success'):
                order_product_attribute_queries = {}
                fields_table = self.getAllColumnInTable('order_option')
                for  key, order_product_option in order_product_options.items():
                    query_key = order_product_option['order_product_id']
                    order_product_id = order_product_import['data'][query_key] if(order_product_import['data'][query_key])   else False
                    if(order_product_id==False):
                        continue
                    
                    order_product_option['order_product_id'] = order_product_id
                    data_insert = order_product_option
                    data_insert = self.syncFieldsInsert(data_insert, fields_table)
                    order_product_attribute_query = "INSERT INTO order_option "
                    order_product_attribute_query = order_product_attribute_query + self.arrayToInsertCondition(data_insert)
                    order_product_attribute_queries['opa' + key] = {'type' : 'insert',
                        'query' : order_product_attribute_query}
                
                if(order_product_attribute_queries):
                    order_product_attribute_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : order_product_attribute_queries})
                
            
        

        # order history
        histories = convert['histories']
        history_queries = {}
        fields_table = self.getAllColumnInTable('order_history')
        for key , history in histories.items():
            orders_status_id = self._notice['map']['order_status'][history['status']] if (self._notice['map']['order_status'][history['status']])  else 1
            
            data_insert = {'order_id' : order_id,
                'order_status_id' : orders_status_id,
                'date_added' : self.convertStringToDatetime(history['created_at']) if history['created_at']  else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'notify' : 1 if history['notified'] else 0,
                'comment' : history['comment'] if history['comment'] else ''}
            history_data = self.syncFieldsInsert(data_insert, fields_table)
            
            history_query = "INSERT INTO order_history "
            history_query = history_query + self.arrayToInsertCondition(history_data)
            query_key = 'osh' + key
            history_queries[query_key] = {'type' : 'insert',
                'query' : history_query}
        
        if(history_queries):
            history_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : history_queries})
        

        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    

    def additionOrderImport(self,order_id, convert, order, ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : }



    def getAllColumnInTable(self,table_name):
        where_con = ''
        if(self.iset(self._notice['target']['config'],'db_name')):
            where_con = " AND TABLE_SCHEMA = '"+self._notice['target']['config']['db_name']."'"
        else:
            file_url = self.getConnectorUrl('file')
            check_config_file = self.getConnectorData(file_url, {'files' : {{'type' : 'content',
                            'path' : 'admin/config.php'}}})
                        
                    
                
            match = re.search('/define\(\'DB_DATABASE\', \'(.+)\'\)/', check_config_file['data'][0])
            if(match):
                self._notice['target']['config']['db_name'] = str(match[1])
                where_con = " AND TABLE_SCHEMA = '"+self._notice['target']['config']['db_name']+"'"
            
            
        
        fields_table = "select DISTINCT(column_name) from information_schema.columns where table_name='{table_name}' AND (`COLUMN_KEY` <> 'PRI') "+where_con
        fields = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : fields_table}})
            
        
        fields_return = []
        if(isinstance(fields["data"],(frozenset, list, set, tuple))):
            for field in fields["data"]:
                fields_return.append(field['column_name'])
            
        
        return fields_return
    
        
    






