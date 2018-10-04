import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading
import math

#from cartmvc import Cartmvc
from config import ConfigInfo

class Oscommerce(ConfigInfo):
   #'Common base class for all employees'
 
    def ConfigSource(self):

        response = self._defaultResponse()
        #var_dump(response)exit()
        default_config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT cfg.*, lg.* FROM configuration AS cfg LEFT JOIN languages AS lg ON lg.code = cfg.configuration_value WHERE cfg.configuration_key = 'DEFAULT_LANGUAGE'"},
                'currencies' : {'type' : 'select',
                    'query' : "SELECT cfg.*, cur.* FROM configuration AS cfg LEFT JOIN currencies AS cur ON cur.code = cfg.configuration_value WHERE cfg.configuration_key = 'DEFAULT_CURRENCY'"}}})
            
        
        #var_dump(default_config)exit()
        if(default_config or default_config['result'] != "success"):
            return self.errorConnector(False)
        
        default_config_data = default_config['data']
        if(default_config_data and default_config_data['languages'] and default_config_data['currencies']):
            self._notice['src']['language_default'] =  1
            self._notice['src']['currency_default'] =  1
        
        self._notice['src']['category_root'] = 1
        self._notice['src']['site'] = {1 : 'Default Shop'}
        self._notice['src']['categoryData'] = {1 : 'Default Category'}
        
        self._notice['src']['attributes'] = {1 : 'Default Attribute'}
        
        config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : True,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT * FROM languages"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT * FROM currencies"},
                
                'orders_status' : {'type' : 'select',
                    'query' : "SELECT * FROM orders_status WHERE language_id = '" + self._notice['src']['language_default'] + "'"}}})
                
            
        
        if(config == False or config['result'] != "success"):
            return self.errorConnector(False)
        
        config_data = config['data']
        language_data = currency_data = order_status_data = {}
        for language_row in config_data['languages'].items():
            lang_id = language_row['languages_id']
            lang_name = language_row['name'] + "(" + language_row['code'] + ")"
            language_data[lang_id] = lang_name
        
        for order_status_row in config_data['orders_status'].items():
            order_status_id = order_status_row['orders_status_id']
            order_status_name = order_status_row['orders_status_name']
            order_status_data[order_status_id] = order_status_name
        
        for currency_row in config_data['currencies'].items():
            currency_id = currency_row['currencies_id']
            currency_name = currency_row['title']
            currency_data[currency_id] = currency_name
        
        self._notice['src']['languages'] = language_data
        self._notice['src']['order_status'] = order_status_data
        self._notice['src']['currencies'] = currency_data
        self._notice['src']['support']['country_map'] = False
        self._notice['src']['support']['customer_group_map'] = False
        response['result'] = 'success'
        return response
    

    def ConfigTarget(self):

        response = self._defaultResponse()
        default_config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT cfg.*, lg.* FROM configuration AS cfg LEFT JOIN languages AS lg ON lg.code = cfg.configuration_value WHERE cfg.configuration_key = 'DEFAULT_LANGUAGE'"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT cfg.*, cur.* FROM configuration AS cfg LEFT JOIN currencies AS cur ON cur.code = cfg.configuration_value WHERE cfg.configuration_key = 'DEFAULT_CURRENCY'"}}})
                
            
        
        if(default_config == False or default_config['result'] != "success"):
            return self.errorConnector(False)
        
        default_config_data = default_config['data']
        if(default_config_data and default_config_data['languages'] and default_config_data['currencies']):
            self._notice['target']['language_default'] =  1
            self._notice['target']['currency_default'] =  1
        
        self._notice['target']['category_root'] = 1
        self._notice['target']['support']['site_map'] = False
        self._notice['target']['support']['category_map'] = False
        self._notice['target']['support']['attribute_map'] = False
        config = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : True,
            'query' : {'languages' : {'type' : 'select',
                    'query' : "SELECT * FROM languages"},
                
                'currencies' : {'type' : 'select',
                    'query' : "SELECT * FROM currencies"},
                
                'orders_status' : {'type' : 'select',
                    'query' : "SELECT * FROM orders_status WHERE language_id = '" + self._notice['target']['language_default'] + "'"}}})
                
            
        
        if(config == False or config['result'] != "success"):
            return self.errorConnector(False)
        
        config_data = config['data']
        language_data = order_status_data = currency_data = country_data = {}
        for language_row in config_data['languages'].items():
            lang_id = language_row['languages_id']
            lang_name = language_row['name'] + "(" + language_row['code'] + ")"
            language_data[lang_id] = lang_name
        
        for order_status_row in config_data['orders_status'].items():
            order_status_id = order_status_row['orders_status_id']
            order_status_name = order_status_row['orders_status_name']
            order_status_data[order_status_id] = order_status_name
        
        for currency_row in config_data['currencies'].items():
            currency_id = currency_row['currencies_id']
            currency_name = currency_row['title']
            currency_data[currency_id] = currency_name
        
        countries = self.getCountriesTarget()
        for country_row in countries.items():
            country_id = country_row['countries_id']
            country_name = country_row['countries_name']
            country_data[country_id] = country_name
        
        self._notice['target']['support']['language_map'] = True
        self._notice['target']['languages'] = language_data
        self._notice['target']['support']['order_status_map'] = True
        self._notice['target']['order_status'] = order_status_data
        self._notice['target']['support']['currency_map'] = True
        self._notice['target']['currencies'] = currency_data
        self._notice['target']['support']['country_map'] = True
        self._notice['target']['countries'] = country_data
        self._notice['target']['support']['customer_group_map'] = False
        self._notice['target']['support']['taxes'] = True
        self._notice['target']['support']['manufacturers'] = True
        self._notice['target']['support']['categories'] = True
        self._notice['target']['support']['products'] = True
        self._notice['target']['support']['customers'] = True
        self._notice['target']['support']['orders'] = True
        self._notice['target']['support']['reviews'] = True
        response['result'] = 'success'
        return response
    







    #migration taxe
    def getTaxesMainExport(self):
        last_id_f = self.get_id_last('Taxe')
        query = "SELECT * FROM taxes where tax_class_id >%d and tax_class_id < %d"  %(last_id_f,last_id_f+12)
        query = {'query':{'type':'select', 'query':query},}
        #url = self.notice['src']['cart_url'] + 'cartmigration_connector/connector.php?action=query&token=' + self.notice['src']['config']['token']
        url = self.getConnectorUrl('query')
        taxes = self.getConnectorData(url, query)
        #print(customers)
        return taxes


    def getTaxesExtExport(self,taxes):
        taxIds = self.duplicateFieldValueFromList(taxes['data'], 'tax_class_id')
        tax_id_con = self.arrayToInCondition(taxIds)
        taxes_ext_queries = {'tax_rates' : {'type' : 'select',
                'query' : "SELECT * FROM tax_rates WHERE tax_class_id IN " + tax_id_con}}
        # add custom
        taxesExt = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : taxes_ext_queries})
        if(taxesExt == False or taxesExt['result'] != 'success'):
            return self.errorConnector()

        taxZoneIds = self.duplicateFieldValueFromList(taxesExt['data']['tax_rates'], 'tax_zone_id')
        tax_zone_query = self.arrayToInCondition(taxZoneIds)
        taxes_ext_rel_queries = {'geo_zones' : {
                'type' : 'select',
                'query' : "SELECT * FROM geo_zones WHERE geo_zone_id IN " + tax_zone_query},
            'zones_to_geo_zones' : {
                'type' : 'select',
                'query' : "SELECT ztgz.*, z.*, c.*FROM zones_to_geo_zones AS ztgz LEFT JOIN zones AS z ON z.zone_id = ztgz.zone_id LEFT JOIN countries AS c ON c.countries_id = ztgz.zone_country_id WHERE ztgz.geo_zone_id IN " + tax_zone_query}}
        #add custom
        taxesExtRel = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : taxes_ext_rel_queries})
        if(taxesExtRel == False or taxesExtRel['result'] != 'success'):
            return self.errorConnector()
        taxesExt = self.syncConnectorObject(taxesExt, taxesExtRel)
        return taxesExt



    def convertTaxExport(self,tax, taxesExt):
        taxProduct = taxCustomer = taxZone = []

        tax_product = self.constructTaxProduct()
        tax_product = self.addConstructDefault(tax_product)
        tax_product['id'] = tax['tax_class_id']
        tax_product['code'] = None
        tax_product['name'] = tax['tax_class_title']
        tax_product['created_at'] = tax['date_added']
        tax_product['updated_at'] = tax['last_modified']
        taxProduct.append(tax_product)

        srcTaxRate = self.getListFromListByField(taxesExt['data']['tax_rates'], 'tax_class_id', tax['tax_class_id'])
        for src_tax_rate in srcTaxRate:
            tax_zone_rate = self.constructTaxZoneRate()
            tax_zone_rate = self.addConstructDefault(tax_zone_rate)
            tax_zone_rate['id'] = src_tax_rate['tax_rates_id']
            tax_zone_rate['name'] = src_tax_rate['tax_description']
            tax_zone_rate['rate'] = src_tax_rate['tax_rate']
            tax_zone_rate['priority'] = src_tax_rate['tax_priority']

            srcTaxZone = self.getListFromListByField(taxesExt['data']['zones_to_geo_zones'], 'geo_zone_id', src_tax_rate['tax_zone_id'])
            for src_tax_zone in srcTaxZone:
                tax_zone_state = self.constructTaxZoneState()
                tax_zone_state = self.addConstructDefault(tax_zone_state)
                tax_zone_state['id'] = src_tax_zone['zone_id']
                tax_zone_state['name'] = src_tax_zone['zone_name']
                tax_zone_state['state_code'] = src_tax_zone['zone_code']

                tax_zone_country = self.constructTaxZoneCountry()
                tax_zone_country = self.addConstructDefault(tax_zone_country)
                tax_zone_country['id'] = src_tax_zone['zone_country_id']
                tax_zone_country['name'] = src_tax_zone['countries_name']
                tax_zone_country['country_code'] = src_tax_zone['countries_iso_code_2']

                src_geo_zone = self.getRowFromListByField(taxesExt['data']['geo_zones'], 'geo_zone_id', src_tax_zone['geo_zone_id'])
                tax_zone = self.constructTaxZone()
                tax_zone = self.addConstructDefault(tax_zone)
                tax_zone['id'] = src_tax_zone['geo_zone_id']
                tax_zone['name'] = src_geo_zone['geo_zone_name']
                tax_zone['country'] = tax_zone_country
                tax_zone['state'] = tax_zone_state
                tax_zone['rate'] = tax_zone_rate

                taxZone.append(tax_zone)

        tax_data = self.constructTax()
        tax_data = self.addConstructDefault(tax_data)
        tax_data['id'] = tax['tax_class_id']
        tax_data['name'] = tax['tax_class_title']
        tax_data['created_at'] = tax['date_added']
        tax_data['updated_at'] = tax['last_modified']
        tax_data['tax_products'] = taxProduct
        tax_data['tax_zones'] = taxZone

        return {'result' : 'success',
            'msg' : '',
            'data' : tax_data}

    def getTaxIdImport(self,convert):
        return convert['id']


    def routerTaxImport(self,convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'} #taxImport - beforeTaxImport - additionTaxImport
    

    def beforeTaxImport(self, convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def taxImport(self,convert, tax, taxesExt):

        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_target = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')

        taxClassId = geoZoneId = zoneToGeoZoneId  = []
        taxProductImport = {}

        ## tax class
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
                    taxClassId.append(tax_product_exists['id_desc'])
                    continue
                
            
            taxProductImport[query_key] = {'id' : tax_product['id'],
                'code' : tax_product['code']}
            tax_product_data = {'tax_class_title' : tax_product['name'],
                'last_modified' : tax_product['updated_at'],
                'date_added' : tax_product['created_at']}

            tax_product_data = self.oscDatetimeRequire(tax_product_data, 'date_added')
            query = "INSERT INTO tax_class "
            query = query + self.arrayToInsertCondition(tax_product_data)
            taxProductQueries[query_key] = {'type' : 'insert',
                'query' : query,
                'params' : {'insert_id' : True}}
                
            
        
        taxProductResponse = self.getConnectorData(url_query, {'serialize' : False,
            'query' : taxProductQueries})

        if(taxProductResponse == False or taxProductResponse['result'] != 'success'):
            response['result'] = 'warning'
            response['msg'] = 'Tax class import failed'
            return response
        
        for query_key, tax_product_data in taxProductImport.items():
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
        for  tax_zone_id in taxZoneId.items():
            taxZoneJoin = self.getListFromListByField(taxZone, 'id', tax_zone_id)
            geo_zone_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone_id)
            geo_zone_id = None
            geo_zone_map_save = False
            if(geo_zone_exists):
                geo_zone_id = geo_zone_exists['id_desc']
                geo_zone_map_save = True
            
            for tax_zone in taxZoneJoin.items():
                if(geo_zone_id == False):
                    tax_zone_data = {'geo_zone_name' : tax_zone['name'],
                        'geo_zone_description' : ' ',
                        'last_modified' : tax_zone['updated_at'],
                        'date_added' : tax_zone['created_at']}
                    tax_zone_data = self.oscDatetimeRequire(tax_zone_data, 'date_added')
                    geo_zone_query = "INSERT INTO geo_zones "
                    geo_zone_query += self.arrayToInsertCondition(tax_zone_data)
                    geo_zone_import = self.getConnectorData(url_query, {'query' : serialize({'type' : 'insert',
                                'query' : geo_zone_query,
                                'params' : {'insert_id' : True}})})
                                
                                    
                        
                    
                    if(geo_zone_import == False or geo_zone_import['result'] != 'success' or geo_zone_import['data'] == False):
                        response['result'] = 'warning'
                        response['msg'] = 'Tax geo zone import failed.'
                        return response
                    
                    geo_zone_id = geo_zone_import['data']
                    conn = self.connectdb()
                    query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone['id'], tax_zone['code'], geo_zone_id)
                    self.insertdb(conn,query)
                    #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone['id'], tax_zone['code'], geo_zone_id)
                

                if(geo_zone_map_save == False):
                    tax_zone_country = tax_zone['country']
                    tax_zone_state = tax_zone['state']
                    tax_zone_date_added = tax_zone['created_at']
                    if(tax_zone_date_added == False):
                        tax_zone_date_added = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    zone_to_geo_zone_query = "INSERT INTO zones_to_geo_zones (zone_country_id, zone_id, geo_zone_id, last_modified, date_added) VALUES ((SELECT countries_id FROM countries WHERE countries_iso_code_2 = " + tax_zone_country['code'] + "), (SELECT z.zone_id FROM zones AS z LEFT JOIN countries AS c ON c.countries_id = z.zone_country_id WHERE c.countries_iso_code_2 = " + tax_zone_country['code'] + " AND z.zone_code = " + tax_zone_state['code'] + "), " + geo_zone_id + ", " + tax_zone['updated_at'] + ", " + tax_zone_date_added + ")"
                    zone_to_geo_zone_import = self.getConnectorData(url_query, {'query' : serialize({'type' : 'insert',
                            'query' : zone_to_geo_zone_query})})
                        
                    
                    

                tax_zone_rate = tax_zone['rate']
                tax_zone_rate_id = None
                if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                    conn = self.connectdb()
                    query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d and code_src = '%s'""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], None, tax_zone_rate['code'])
                    tax_zone_rate_exists = self.selectdb(conn,query)
                    #tax_zone_rate_exists = conn.insert_id()
                    #tax_zone_rate_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], None, tax_zone_rate['code'])
                    if(tax_zone_rate_exists):
                        tax_zone_rate_id = tax_zone_rate_exists['id_desc']
                        
                
                if(tax_zone_rate_id == False):

                    tax_zone_rate_queries = {}
                    for key, tax_class_id in enumerate(taxClassId):
                        tax_zone_rate_data = {'tax_zone_id' : geo_zone_id,
                            'tax_class_id' : tax_class_id,
                            'tax_priority' : tax_zone_rate['priority'],
                            'tax_rate' : tax_zone_rate['rate'],
                            'tax_description' : tax_zone_rate['name'],
                            'last_modified' : tax_zone_rate['updated_at'],
                            'date_added' : tax_zone_rate['created_at']}

                        
                        tax_zone_rate_data = self.oscDatetimeRequire(tax_zone_rate_data, 'date_added')
                        tax_zone_rate_query = "INSERT INTO tax_rates "
                        tax_zone_rate_query = tax_zone_rate_query + self.arrayToInsertCondition(tax_zone_rate_data)
                        query_key = 'tax_zone_rate_' + key
                        tax_zone_rate_queries[query_key] = {'type' : 'insert',
                            'query' : tax_zone_rate_query,
                            'params' : {'insert_id' : True}}
                           
                        
                    
                    if(tax_zone_rate_queries):
                        tax_zone_rate_import = self.getConnectorData(url_query, {'serialize' : False,
                            'query' : tax_zone_rate_queries})
                    
                        if(tax_zone_rate_import == False or tax_zone_rate_import['result'] != 'success' or tax_zone_rate_import['data'] == False):
                            response['result'] = 'warning'
                            response['msg'] = 'Tax rate import failed'
                            return response
                        
                        tax_zone_rate_id = tax_zone_rate_import['data']['tax_zone_rate_0']
                        if(tax_zone_rate_id):
                            if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                                conn = self.connectdb()
                                query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                                self.insertdb(conn,query)
                                #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                            
                        
                    
                
            
        

        taxZoneNoId = self.getListFromListByField(taxZone, 'id', None)
        if(taxZoneNoId):
            taxZoneCode = self.duplicateFieldValueFromList(taxZoneNoId, 'code')
            for tax_zone_code in taxZoneCode.items():
                taxZoneJoin = self.getListFromListByField(taxZone, 'code', tax_zone_code)
                conn = self.connectdb()
                query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d and code_src = '%s'""" %(uurl_src, url_target, self.TYPE_TAX_ZONE, None, None, tax_zone_code)
                geo_zone_exists = self.selectdb(conn,query)
                #geo_zone_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_ZONE, None, None, tax_zone_code)
                geo_zone_id = None
                geo_zone_map_save = False
                if(geo_zone_exists):
                    geo_zone_id = geo_zone_exists
                    geo_zone_map_save = True
                
                for tax_zone in taxZoneJoin.items():
                    if(geo_zone_id == False):
                        tax_zone_data = {'geo_zone_name' : tax_zone['name'],
                            'geo_zone_description' : ' ',
                            'last_modified' : tax_zone['updated_at'],
                            'date_added' : tax_zone['created_at']}
                        
                        tax_zone_data = self.oscDatetimeRequire(tax_zone_data, 'date_added')
                        geo_zone_query = "INSERT INTO geo_zones "
                        geo_zone_query = geo_zone_query +  self.arrayToInsertCondition(tax_zone_data)
                        geo_zone_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                    'query' : geo_zone_query,
                                    'params' : {'insert_id' : True}}})
                                    
                                
                            
                        
                        if(geo_zone_import ==False or geo_zone_import['result'] != 'success' or geo_zone_import['data'] == False):
                            response['result'] = 'warning'
                            response['msg'] = 'Tax geo zone import failed.'
                            return response
                        
                        geo_zone_id = geo_zone_import['data']
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone['id'], tax_zone['code'], geo_zone_id)
                        self.insertdb(conn,query)
                        #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE, tax_zone['id'], tax_zone['code'], geo_zone_id)
                    

                    if(geo_zone_map_save):
                        tax_zone_country = tax_zone['country']
                        tax_zone_state = tax_zone['state']
                        tax_zone_date_added = tax_zone['created_at']
                        if(tax_zone_date_added == False):
                            tax_zone_date_added = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        zone_to_geo_zone_query = "INSERT INTO zones_to_geo_zones (zone_country_id, zone_id, geo_zone_id, last_modified, date_added) VALUES ((SELECT countries_id FROM countries WHERE countries_iso_code_2 = " + tax_zone_country['code'] + "), (SELECT z.zone_id FROM zones AS z LEFT JOIN countries AS c ON c.countries_id = z.zone_country_id WHERE c.countries_iso_code_2 = " + tax_zone_country['code'] + " AND z.zone_code = " + tax_zone_state['code'] + "), " + geo_zone_id + ", " + tax_zone['updated_at'] + ", " + tax_zone_date_added + ")"
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
                        for key, tax_class_id in enumerate(taxClassId):
                            tax_zone_rate_data = {'tax_zone_id' : geo_zone_id,
                                'tax_class_id' : tax_class_id,
                                'tax_priority' : tax_zone_rate['priority'],
                                'tax_rate' : tax_zone_rate['rate'],
                                'tax_description' : tax_zone_rate['name'],
                                'last_modified' : tax_zone_rate['updated_at'],
                                'date_added' : tax_zone_rate['created_at']}
                            
                            tax_zone_rate_data = self.oscDatetimeRequire(tax_zone_rate_data, 'date_added')
                            tax_zone_rate_query = "INSERT INTO tax_rates "
                            tax_zone_rate_query += self.arrayToInsertCondition(tax_zone_rate_data)
                            query_key = 'tax_zone_rate_' + key
                            tax_zone_rate_queries[query_key] = {'type' : 'insert',
                                'query' : tax_zone_rate_query,
                                'params' : {'insert_id' : True,}}
                                
                            
                        
                        if(tax_zone_rate_queries):
                            tax_zone_rate_import = self.getConnectorData(url_query, { 'serialize' : False,
                                'query' : tax_zone_rate_queries})
                            
                            if(tax_zone_rate_import == False or tax_zone_rate_import['result'] != 'success' or tax_zone_rate_import['data'] == False):
                                response['result'] = 'warning'
                                response['msg'] = 'Tax rate import failed.'
                                return response
                            
                            tax_zone_rate_id = tax_zone_rate_import['data']['tax_zone_rate_0']
                            if(tax_zone_rate_id):
                                if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                                    conn = self.connectdb()
                                    query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                                    self.insertdb(conn,query)
                                    #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                                
                            
                        
                    
                
            

            taxZoneNoKey = self.getListFromListByField(taxZoneNoId, 'code', None)
            if(taxZoneNoKey):
                for tax_zone in taxZoneNoKey.items():
                    tax_zone_data = {'geo_zone_name' : tax_zone['name'],
                        'geo_zone_description' : ' ',
                        'last_modified' : tax_zone['updated_at'],
                        'date_added' : tax_zone['created_at']}
                    
                    tax_zone_data = self.oscDatetimeRequire(tax_zone_data, 'date_added')
                    geo_zone_query = "INSERT INTO geo_zones "
                    geo_zone_query += self.arrayToInsertCondition(tax_zone_data)
                    geo_zone_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                                'query' : geo_zone_query,
                                'params' : {'insert_id' : True}}})
                                
                            
                        
                    
                    if(geo_zone_import == False or geo_zone_import['result'] != 'success' or geo_zone_import['data'] == False):
                        response['result'] = 'warning'
                        response['msg'] =  'Tax geo zone import failed.'
                        return response
                    
                    geo_zone_id = geo_zone_import['data']

                    tax_zone_country = tax_zone['country']
                    tax_zone_state = tax_zone['state']
                    tax_zone_date_added = tax_zone['created_at']
                    if(tax_zone_date_added == False):
                        tax_zone_date_added = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    zone_to_geo_zone_query = "INSERT INTO zones_to_geo_zones (zone_country_id, zone_id, geo_zone_id, last_modified, date_added) VALUES ((SELECT countries_id FROM countries WHERE countries_iso_code_2 = " + tax_zone_country['code'] + "), (SELECT z.zone_id FROM zones AS z LEFT JOIN countries AS c ON c.countries_id = z.zone_country_id WHERE c.countries_iso_code_2 = " + tax_zone_country['code'] + " AND z.zone_code = " + tax_zone_state['code'] + "), " + geo_zone_id + ", " + tax_zone['updated_at'] + ", " + tax_zone_date_added + ")"
                    zone_to_geo_zone_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                            'query' : zone_to_geo_zone_query}})
                        
                    

                    tax_zone_rate = tax_zone['rate']
                    tax_zone_rate_id = None
                    if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                        conn = self.connectdb()
                        query = """select * from  cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = %d""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], None, tax_zone_rate['code'])
                        tax_zone_rate_exists = self.selectdb(conn,query)
                        #tax_zone_rate_exists = self.selectMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], None, tax_zone_rate['code'])
                        if(tax_zone_rate_exists):
                            tax_zone_rate_id = tax_zone_rate_exists
                        
                    
                    if(tax_zone_rate_id == False):
                        tax_zone_rate_queries = {}
                        for key,tax_class_id in enumerate(taxClassId): 
                            tax_zone_rate_data = {'tax_zone_id' : geo_zone_id,
                                'tax_class_id' : tax_class_id,
                                'tax_priority' : tax_zone_rate['priority'],
                                'tax_rate' : tax_zone_rate['rate'],
                                'tax_description' : tax_zone_rate['name'],
                                'last_modified' : tax_zone_rate['updated_at'],
                                'date_added' : tax_zone_rate['created_at']}
                            
                            tax_zone_rate_data = self.oscDatetimeRequire(tax_zone_rate_data, 'date_added')
                            tax_zone_rate_query = "INSERT INTO tax_rates "
                            tax_zone_rate_query += self.arrayToInsertCondition(tax_zone_rate_data)
                            query_key = 'tax_zone_rate_' + key
                            tax_zone_rate_queries[query_key] = {'type' : 'insert',
                                'query' : tax_zone_rate_query,
                                'params' : {'insert_id' : True}}
                                
                            
                        
                        if(tax_zone_rate_queries):
                            tax_zone_rate_import = self.getConnectorData(url_query, {'serialize' : False,
                                'query' : tax_zone_rate_queries})
                            
                            if(tax_zone_rate_import == False or tax_zone_rate_import['result'] != 'success' or tax_zone_rate_import['data']):
                                response['result'] = 'warning'
                                response['msg'] =  'Tax rate import failed.'
                                return response
                            
                            tax_zone_rate_id = tax_zone_rate_import['data']['tax_zone_rate_0']
                            if(tax_zone_rate_id):
                                if(tax_zone_rate['id'] != None or tax_zone_rate['code'] != None):
                                    conn = self.connectdb()
                                    query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                                    self.insertdb(conn,query)
                                    #self.insertMap(url_src, url_target, self.TYPE_TAX_ZONE_RATE, tax_zone_rate['id'], tax_zone_rate_id, tax_zone_rate['code'])
                                
                            
                        
                    
                
            
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_target, self.TYPE_TAX, convert['id'], 1, convert['code'])
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


    
    def getManufacturersMainExport(self):
        #id_src = self._notice['process']['manufacturers']['id_src']
        limit = self._notice['setting']['manufacturers']
        last_id_f = self.get_id_last('Manufacturer')
        manufacturers = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM manufacturers WHERE manufacturers_id > " + last_id_f + " ORDER BY manufacturers_id ASC LIMIT " + limit}})
        if(manufacturers == False or manufacturers['result'] != 'success'):
            return self.errorConnector()
        return manufacturers






    def getManufacturersExtExport(self,manufacturers):
        url_query = self.getConnectorUrl('query')
        manufacturerId = self.duplicateFieldValueFromList(manufacturers['data'], 'manufacturers_id')
        manufacturer_id_con  = self.arrayToInCondition(manufacturerId)
        manufacturers_ext_queries = {'manufacturers_info' : {'type' : "select",
                'query' : "SELECT * FROM manufacturers_info WHERE manufacturers_id IN " . manufacturer_id_con}}
        # add custom
        manufacturersExt = self.getConnectorData(url_query, {'serialize' : False,
            'query' : manufacturers_ext_queries})
        if(manufacturersExt == False or manufacturersExt['result'] != 'success'):
            return self.errorConnector()
        manufacturers_ext_rel_queries = {}
        # add custom
        if(manufacturers_ext_rel_queries):
            manufacturersExtRel = self.getConnectorData(url_query, {'serialize' : False,
                'query' : manufacturers_ext_rel_queries})
            if(manufacturersExtRel == False or manufacturersExtRel['result'] != 'success'):
                return self.errorConnector()
            manufacturersExt = self.syncConnectorObject(manufacturersExt, manufacturersExtRel)
        return manufacturersExt



    def getCustomersMainExport(self):
        last_id_f = self.get_id_last('Customer')
        query = "SELECT * FROM customers where customers_id >%d and customers_id < %d"  %(last_id_f,last_id_f+12)
        query = {'query':{'type':'select', 'query':query},}
        url = self.notice['src']['cart_url'] + 'catalog/cartmigration_connector/connector.php?action=query&token=' + self.notice['src']['config']['token']
        customers = self.getConnectorData(url, query)
        #print(customers)
        return customers


    def convertManufacturerExport(self,manufacturer,manufacturersExt):
        manufacturer_data = self.constructManufacturer()
        manufacturer_data = self.addConstructDefault(manufacturer_data)
        manufacturer_data['id'] = manufacturer['manufacturers_id']
        manufacturer_data['name'] = manufacturer['manufacturers_name']
        manufacturer_data['image']['url'] = self.getUrlSuffix(self._notice['src']['config']['image_manufacturer'])
        manufacturer_data['image']['path'] = manufacturer['manufacturers_image']
        manufacturerInfo = self.getListFromListByField(manufacturersExt['data']['manufacturers_info'], 'manufacturers_id', manufacturer['manufacturers_id'])
        default_language = self._notice['src']['language_default']
        manufacturer_url = self.getRowFromListByField(manufacturerInfo, 'languages_id', default_language)
        manufacturer_data['url'] = manufacturer_url
        manufacturer_data['created_at'] = manufacturer['date_added']
        manufacturer_data['updated_at'] = manufacturer['last_modified']
        for language_id,language_label in self._notice['src']['languages'].items():
        #foreach(self._notice['src']['languages'] as language_id : language_label){
            manufacturer_language_data = self.constructManufacturerLang()
            manufacturer_language_data['name'] = manufacturer['manufacturers_name']
            manufacturer_data['languages'][language_id] = manufacturer_language_data
        return {'result' : 'success',
            'msg' : '',
            'data' : manufacturer_data}
    

    def getManufacturerIdImport(self,convert, manufacturer, manufacturersExt):
        return convert['id']

    #def checkManufacturerImport(self,convert, manufacturer, manufacturersExt):
        #return self.getMapFieldBySource(self.TYPE_MANUFACTURER, convert['id'], convert['code']) ? True : False

    def routerManufacturerImport(convert, manufacturer, manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'manufacturerImport'}


    def beforeManufacturerImport(convert, manufacturer, manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def manufacturerImport(self,convert, manufacturer, manufacturersExt):
        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_target = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        url_image = self.getConnectorUrl('image')
        manufacturers_image = ' '
        if(convert['image']['path']):
            image_process = self.processImageBeforeImport(convert['image']['url'], convert['image']['path'])
            image_import = self.getConnectorData(url_image, {'images' : {'mi' : {'type' : 'download',
                        'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_manufacturer']),
                        'params' : {'url' : image_process['url'],
                            'rename' : True}}}})
            if(image_import and image_import['result'] == 'success'):
                image_import_path = image_import['data']['mi']
                if(image_import_path):
                    manufacturers_image = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_manufacturer'])

        manufacturer_data = {'manufacturers_name' : convert['name'],
            'manufacturers_image' : manufacturers_image,
            'date_added' : convert['created_at'],
            'last_modified' : convert['updated_at']}
        manufacturer_data = self.oscDatetimeRequire(manufacturer_data, 'date_added')
        manufacturer_query = "INSERT INTO manufacturers "
        manufacturer_query += self.arrayToInsertCondition(manufacturer_data)
        manufacturer_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : manufacturer_query,
                'params' : {'insert_id' : True}}})
        if(manufacturer_import == False or manufacturer_import['result'] != 'success'):
            response['result'] = 'warning'
            response['msg'] = 'warning' #self.warningImportEntity('Manufacturer', convert['id'], convert['code'])
            return response
        manufacturer_id = manufacturer_import['data']
        if(manufacturer_id == False):
            response['result'] = 'warning'
            response['msg'] =  'warning' #self.warningImportEntity('Manufacturer', convert['id'], convert['code'])
            return response

        #self.insertMap(url_src, url_target, self.TYPE_MANUFACTURER, convert['id'], manufacturer_id, convert['code'])
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc) values('%s','%s','%s',%d,%d)""" %(url_src, url_target, manufacturer, convert['id'], manufacturer_id)
        self.insertdb(conn,query)
        return {'result' : 'success',
            'msg' : '',
            'data' : manufacturer_id}



    def afterManufacturerImport(self,manufacturer_id, convert, manufacturer, manufacturersExt):
        manufacturer_info_queries = {}
        for src_language_id, target_language_id in self._notice['map']['languages'].items():
            key = 'manufacturers_info_' + src_language_id
            manufacturer_info_query = "INSERT INTO manufacturers_info "
            manufacturer_info_query = manufacturer_info_query +  self.arrayToInsertCondition({'manufacturers_id' : manufacturer_id,
                'languages_id' : target_language_id,
                'manufacturers_url' : convert['url'],
                'url_clicked' : 0})
            manufacturer_info_queries[key] = {'type' : 'insert',
                'query' : manufacturer_info_query}
        if(manufacturer_info_queries):
            manufacturer_info_import = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
                'query' : manufacturer_info_queries})
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def additionManufacturerImport(self,manufacturer_id, convert, manufacturer, manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}




    #customer
    def constructCustomer(self):
        return {'id' : None,
            'code' : None,
            'site_id' : 1,
            'language_id' : '',
            'username' : '',
            'email' : '',
            'password' : '',
            'first_name' : '',
            'middle_name' : '',
            'last_name' : '',
            'gender' : '',
            'dob' : '',
            'is_subscribed' : False,
            'active' : True,
            'capabilities' : {},
            'created_at' : None,
            'updated_at' : None,
            'address' : {},
            'groups' : {}}










    def getCategoriesMainExport(self):
        last_id_f = self.get_id_last('Category')
        #id_src = self._notice['process']['categories']['id_src']
        limit = self._notice['setting']['categories']
        categories = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM categories WHERE categories_id > " + last_id_f + " ORDER BY categories_id ASC LIMIT " + limit}})
        if(categories == False or categories['result'] != 'success'):
            return self.errorConnector()
        return categories




    def getCategoriesExtExport(self,categories):
        url_query = self.getConnectorUrl('query')
        categoriesId = self.duplicateFieldValueFromList(categories['data'], 'categories_id')
        category_id_con = self.arrayToInCondition(categoriesId)
        categories_ext_queries = {'categories_description' : {'type' : 'select',
                'query' : "SELECT * FROM categories_description WHERE categories_id IN " + category_id_con}}
        # add custom
        categoriesExt = self.getConnectorData(url_query, {'serialize' : False,
            'query' : categories_ext_queries})
        if(categoriesExt == False or categoriesExt['result'] != 'success'):
            return self.errorConnector()
        categories_ext_rel_queries = {}
        #add custom
        if(categories_ext_rel_queries):
            categoriesExtRel = self.getConnectorData(url_query, {'serialize' : False,
                'query' : categories_ext_rel_queries})
            if(categoriesExtRel == False or categoriesExtRel['result'] != 'success'):
                return self.errorConnector()
            categoriesExt = self.syncConnectorObject(categoriesExt, categoriesExtRel)
        return categoriesExt


    
    def convertCategoryExport(self,category, categoriesExt):
        response = self._defaultResponse()
        category_data = self.constructCategory()
        category_data = self.addConstructDefault(category_data)
        parent = self.constructCategoryParent()
        parent = self.addConstructDefault(parent)
        if(category['parent_id']):
            parent = self.getCategoryParent(category['parent_id'])
            if(parent['result'] != 'success'):
                response['result'] = 'warning'
                response['msg'] = 'warning! Could not get parent data.'
                return response
            parent = parent['data']
        else:
            parent['id'] = 0
        category_data['id'] = category['categories_id']
        category_data['parent'] = parent
        category_data['active'] = True
        category_data['image']['url'] = self.getUrlSuffix(self._notice['src']['config']['image_category'])
        category_data['image']['path'] = category['categories_image']
        category_data['sort_order'] = category['sort_order']
        category_data['created_at'] = category['date_added']
        category_data['updated_at'] = category['last_modified']
        category_data['category'] = category
        category_data['categoriesExt'] = categoriesExt

        categoryDescription = self.getListFromListByField(categoriesExt['data']['categories_description'], 'categories_id', category['categories_id'])
        language_default = self._notice['src']['language_default']
        categoryDescriptionDef = self.getRowFromListByField(categoryDescription, 'language_id', language_default)
        if(categoryDescriptionDef == False):
            if(categoryDescription[0] in vars()):
                categoryDescriptionDef =   categoryDescription[0]
            else:
                categoryDescriptionDef = False      
            #categoryDescriptionDef = isset(categoryDescription[0]) ? categoryDescription[0] : False
        if(categoryDescriptionDef == False):
            response['result'] = 'warning'
            response['msg'] = 'Category data error.'
            return response

        category_data['name'] = categoryDescriptionDef['categories_name']

        for language_id,language_label in self._notice['src']['languages'].items():
            category_language_data = self.constructCategoryLang()
            categoryDescriptionLang = self.getRowFromListByField(categoryDescription, 'language_id', language_id)
            if(categoryDescriptionLang):
                category_language_data['name'] = categoryDescriptionLang['categories_name']
                category_data['languages'][language_id] = category_language_data

        return {'result' : 'success',
            'msg' : '',
            'data' : category_data}

    

    def getCategoryIdImport(self,convert, category, categoriesExt):
        return convert['id']


    def routerCategoryImport(self,convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'categoryImport'}


    def beforeCategoryImport(self,convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def categoryImport(self,convert, category, categoriesExt):
        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        url_image = self.getConnectorUrl('image')
        if(convert['parent']['id'] == False and convert['parent']['code'] == False):
            parent_id = 0
        else:
            parent_import = self._importCategoryParent(convert['parent'])
            if(parent_import['result'] != 'success'):
                response['result'] = 'warning'
                response['msg'] = 'category parent import failed.'
                return response

            parent_id = parent_import['data']

        categories_image = ' '
        if(convert['image']['path']):
            image_process = self.processImageBeforeImport(convert['image']['url'], convert['image']['path'])
            image_import = self.getConnectorData(url_image, {'images' : {'ci' : {'type' : 'download',
                        'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_category']),
                        'params' : {'url' : image_process['url'],
                            'rename' : True}}}})

            if(image_import and image_import['result'] == 'success'):
                image_import_path = image_import['data']['ci']
                if(image_import_path):
                    categories_image = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_category'])

        category_data = {'categories_image' : categories_image,
            'parent_id' : parent_id,
            'sort_order' : convert['sort_order'],
            'date_added' : convert['created_at'],
            'last_modified' : convert['updated_at']}

        category_data = self.oscDatetimeRequire(category_data, 'date_added')
        category_query = "INSERT INTO categories "
        category_query += self.arrayToInsertCondition(category_data)
        category_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : category_query,
                'params' : {'insert_id' : True}}})

        if(category_import == False or category_import['result'] != 'success'):
            response['result'] = 'warning'
            response['msg'] = self.warningImportEntity('Category', convert['id'], convert['code'])
            return response

        category_id = category_import['data']
        if(category_id == False):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src,url_desc,category,convert['id'],category_id,convert['code'])
        self.insertdb(conn,query)
        #self.insertMap(url_src, url_desc, self.TYPE_CATEGORY, convert['id'], category_id, convert['code'])
        return {'result' : 'success',
            'msg' : '',
            'data' : category_id}

    def afterCategoryImport(self,category_id, convert, category, categoriesExt):
        url_query = self.getConnectorUrl('query')
        categories_description_queries = {}
        for src_language_id,target_language_id in self._notice['map']['languages'].items():
            if(self.iset(category['languages'],src_language_id)):
                categoryLang = category['languages'][src_language_id]
            else:
                categoryLang = None
            #categoryLang = isset(category['languages'][src_language_id]) ? category['languages'][src_language_id] : None
            if(categoryLang):
                key = 'categories_description_' + src_language_id
                categories_description_query = "INSERT INTO categories_description "
                categories_description_query = categories_description_query + self.arrayToInsertCondition({'categories_id' : category_id,
                    'language_id' : target_language_id,
                    'categories_name' : categoryLang['name']})
                categories_description_queries[key] = {'type' : 'insert',
                    'query' : categories_description_query}

        if(categories_description_queries):
            categories_description_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : categories_description_queries})
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def additionCategoryImport(self,category_id, convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    





    def getProductsMainExport(self,last_id_f):
        last_id_f = self.get_id_last('Product')
        query = "SELECT * FROM products where products_id >%d and products_id < %d"  %(last_id_f,last_id_f+12)
        query = {'query':{'type':'select', 'query':query},}
        url = self.notice['src']['cart_url'] + 'catalog/cartmigration_connector/connector.php?action=query&token=' + self.notice['src']['config']['token']
        products = self.getConnectorData(url, query)
        #print(customers)
        return products

    def getProductsExtExport(self,products):
        url_query = self.getConnectorUrl('query')
        productId = self.duplicateFieldValueFromList(products['data'], 'products_id')
        manufacturerId = self.duplicateFieldValueFromList(products['data'], 'manufacturers_id')
        product_id_con = self.arrayToInCondition(productId)
        manufacturer_id_con = self.arrayToInCondition(manufacturerId)
        product_ext_queries = {'products_description' : {'type' : "select",
                'query' : "SELECT * FROM products_description WHERE products_id IN " + product_id_con},
            'products_to_categories' : {'type' : 'select',
                'query' : "SELECT * FROM products_to_categories WHERE products_id IN " + product_id_con},
            'products_images' : {'type' : 'select',
                'query' : "SELECT * FROM products_images WHERE products_id IN " + product_id_con},
            'products_attributes' : {'type' : 'select',
                'query' : "SELECT * FROM products_attributes WHERE products_id IN " + product_id_con},
            'specials' : {'type' : 'select',
                'query' : "SELECT * FROM specials WHERE products_id IN " + product_id_con},
            'manufacturers' : {'type' : 'select',
                'query' : "SELECT manufacturers_id, manufacturers_name FROM manufacturers WHERE manufacturers_id IN " + manufacturer_id_con}}
        # add custom
        productsExt = self.getConnectorData(url_query, {'serialize' : False,
            'query' : product_ext_queries})
        if(productsExt == False or productsExt['result'] != 'success'):
            return self.errorConnector()
            
        optionId = self.duplicateFieldValueFromList(productsExt['data']['products_attributes'], 'options_id')
        optionValueId = self.duplicateFieldValueFromList(productsExt['data']['products_attributes'], 'options_values_id')
        option_id_con = self.arrayToInCondition(optionId)
        option_value_id_con = self.arrayToInCondition(optionValueId)
        product_ext_rel_queries = {'products_options' : {'type' : 'select',
                'query' : "SELECT * FROM products_options WHERE products_options_id IN " + option_id_con},
            'products_options_values' : {'type' : 'select',
                'query' : "SELECT * FROM products_options_values WHERE products_options_values_id IN " + option_value_id_con}}
        # add custom
        productsExtRel = self.getConnectorData(url_query, {'serialize' : False,
            'query' : product_ext_rel_queries})
        if(productsExtRel == False or productsExtRel['result'] != 'success'):
            return self.errorConnector()
        productsExt = self.syncConnectorObject(productsExt, productsExtRel)
        return productsExt


    def convertProductExport(self,product, productsExt):
        #var_dump(product)exit()
        response = self._defaultResponse()
        product_data = self.constructProduct()
        product_data = self.addConstructDefault(product_data)
        product_data['id'] = product['products_id']
        product_data['type'] = self.PRODUCT_SIMPLE
        product_data['sku'] = product['products_model']
        product_data['price'] = product['products_price']
        product_data['weight'] = product['products_weight']
        product_data['status'] = product['products_status']
        product_data['qty'] = product['products_quantity']
        product_data['manage_stock'] = True
        product_data['created_at'] = product['products_date_added']
        product_data['updated_at'] = product['products_date_available']

        productDescription = self.getListFromListByField(productsExt['data']['products_description'], 'products_id', product['products_id'])
        language_default = self._notice['src']['language_default']
        productDescriptionDef = self.getRowFromListByField(productDescription, 'language_id', language_default)
        if(productDescriptionDef == False):
            if(productDescription[0] in vars()):
                productDescriptionDef = productDescription[0]
            else:
                productDescriptionDef = False
            #productDescriptionDef = isset(productDescription[0]) ? productDescription[0] : False

        product_data['name'] = productDescriptionDef['products_name']
        product_data['description'] = productDescriptionDef['products_description']
        product_data['short_description'] = productDescriptionDef['products_description']

        url_product_image = self.getUrlSuffix(self._notice['src']['config']['image_product'])
        product_data['image']['url'] = url_product_image
        product_data['image']['path'] = product['products_image']

        productImage = self.getListFromListByField(productsExt['data']['products_images'], 'products_id', product['products_id'])
        #echo productImageexit()
        #var_dump(productImage)exit()
        if(productImage):
            for product_image in productImage.items():
                #var_dump(product_image)exit()
                product_image_data = self.constructProductImage()
                product_image_data['label'] = product_image['htmlcontent']
                product_image_data['url'] = url_product_image
                product_image_data['path'] = product_image['image']
                product_data['images'].append(product_image_data)
                #var_dump(product_data['images'])exit()


        special = self.getRowFromListByField(productsExt['data']['specials'], 'products_id', product['products_id'])
        if(special):
            product_data['special_price']['price'] = special['specials_new_products_price']
            product_data['special_price']['start_date'] = special['specials_date_added']
            product_data['special_price']['end_date'] = special['expires_date']

        product_data['tax']['id'] = product['products_tax_class_id']

        if(product['manufacturers_id']):
            product_data['manufacturer']['id'] = product['manufacturers_id']
            manufacturer = self.getRowFromListByField(productsExt['data']['manufacturers'], 'manufacturers_id', product['manufacturers_id'])
            if(manufacturer):
                product_data['manufacturer']['name'] = manufacturer['manufacturers_name']
            
        

        productCategory = self.getListFromListByField(productsExt['data']['products_to_categories'], 'products_id', product['products_id'])
        if(productCategory):
            for product_category in productCategory.items():
                product_category_data = self.constructProductCategory()
                product_category_data['id'] = product_category['categories_id']
                product_data['categories'].append(product_category_data)
        

        for product_description in productDescription.items():
            product_language_data = self.constructProductLang()
            product_language_data['name'] = product_description['products_name']
            product_language_data['description'] = product_description['products_description']
            product_language_data['short_description'] = product_description['products_description']
            language_id = product_description['language_id']
            product_data['languages'][language_id] = product_language_data


        productAttribute = self.getListFromListByField(productsExt['data']['products_attributes'], 'products_id', product['products_id'])
        if(productAttribute):
            optionId = self.duplicateFieldValueFromList(productAttribute, 'options_id')
            for option_id in optionId.items():
                option_data = self.constructProductOption()
                option_data['id'] = option_id
                option_data['option_type'] = 'select'
                option_data['option_mode'] = self.OPTION_BACKEND

                productOption = self.getListFromListByField(productsExt['data']['products_options'], 'products_options_id', option_id)
                productOptionDef = self.getRowFromListByField(productOption, 'language_id', language_default)
                if(productOptionDef == False):
                    productOptionDef = productOption[0]
                
                option_data['option_name'] = productOptionDef['products_options_name']

                for product_option in productOption.items():
                    option_language_data = self.constructProductOptionLang()
                    option_language_data['option_name'] = product_option['products_options_name']
                    language_id = product_option['language_id']
                    option_data['option_languages'][language_id] = option_language_data
                

                productAttributeOfOption = self.getListFromListByField(productAttribute, 'options_id', option_id)
                optionValueId = self.duplicateFieldValueFromList(productAttributeOfOption, 'options_values_id')
                for option_value_id in optionValueId.items():
                    option_value_data = self.constructProductOptionValue()
                    option_value_data['id'] = option_value_id

                    productOptionValue = self.getListFromListByField(productsExt['data']['products_options_values'], 'products_options_values_id', option_value_id)
                    productOptionValueDef = self.getRowFromListByField(productOptionValue, 'language_id', language_default)
                    if(productOptionValueDef == False):
                        productOptionValueDef = productOptionValue[0]
                    
                    option_value_data['option_value_name'] = productOptionValueDef['products_options_values_name']

                    for product_option_value in productOptionValue.items():
                        option_value_language_data = self.constructProductOptionValueLang()
                        option_value_language_data['option_value_name'] = product_option_value['products_options_values_name']
                        language_id = product_option_value['language_id']
                        option_value_data['option_value_languages'][language_id] = option_value_language_data
                    

                    product_attribute = self.getRowFromListByField(productAttributeOfOption, 'options_values_id', option_value_id)
                    option_value_data['price'] = product_attribute['options_values_price']
                    option_value_data['price_prefix'] = product_attribute['price_prefix']

                    option_data['values'].append(option_value_data)
                
                product_data['options'].append(option_data)
            
        

        return {'result' : 'success',
            'msg' : '',
            'data' : product_data}
    

    def getProductIdImport(self,convert, product, productsExt):

        return convert['id']

    #def checkProductImport(self,convert, product, productsExt):
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

        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')
        url_image = self.getConnectorUrl('image')

        products_image = ' '
        if(convert['image']['path']):
            image_process = self.processImageBeforeImport(convert['image']['url'], convert['image']['path'])
            #var_dump(image_process)exit()
            ##var_dump(url_image)
            ##var_dump(image_process['url'])
            ##var_dump(image_process['path'])
            ##var_dump(self._notice['target']['config']['image_product'])exit()
            image_import = self.getConnectorData(url_image, {'images' : {'pi' : {'type' : 'download',
                        'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_product']),
                        'params' : {'url' : image_process['url'],
                            'rename' : True}}}})

            if(image_import and image_import['result'] == 'success'):
                image_import_path = image_import['data']['pi']
                if(image_import_path):
                    products_image = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_product'])


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


        product_data = {'products_quantity' : convert['qty'],
            'products_model' : convert['sku'],
            'products_image' : products_image,
            'products_price' : convert['price'],
            'products_date_added' : convert['created_at'],
            'products_last_modified' : convert['updated_at'],
            'products_weight' : convert['weight'],
            'products_status' : 1 if convert['status'] else 0,
            'products_tax_class_id' : tax_product_id,
            'manufacturers_id' : manufacturer_id}
        product_data = self.oscDatetimeRequire(product_data, 'products_date_added')
        product_query = "INSERT INTO products "
        product_query += self.arrayToInsertCondition(product_data)

        product_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : product_query,
                'params' : {'insert_id' : True}}})

        if(product_import == False or product_import['result'] != 'success'):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        product_id = product_import['data']

        if(product_id == False):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        #self.insertMap(url_src, url_desc, self.TYPE_PRODUCT, convert['id'], product_id, convert['code'])
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc) values('%s','%s','%s',%d,%d)""" %(url_src, url_target, manufacturer, convert['id'], manufacturer_id)
        self.insertdb(conn,query)
        return {'result' : 'success',
            'msg' : '',
            'data' : product_id}

    def afterProductImport(product_id, convert, product, productsExt):
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        url_query = self.getConnectorUrl('query')

        # description
        products_description_queries = {}
        for src_language_id, desc_language_id in self._notice['map']['languages'].items():
            if (self.iset(convert['languages'],src_language_id)):
                productDescriptionLang = convert['languages'][src_language_id]
            else:
                productDescriptionLang = convert
            #productDescriptionLang = isset(convert['languages'][src_language_id]) ? convert['languages'][src_language_id] : convert
            products_description_query = "INSERT INTO products_description "
            products_description_query += self.arrayToInsertCondition({'products_id' : product_id,
                'language_id' : desc_language_id,
                'products_name' : productDescriptionLang['name'],
                #'products_description' : self.changeImgSrcInText(productDescriptionLang['description'], self._notice['config']['img_des'], self._notice['target']['config']['image_product']),
                'products_url' : ' ',
                'products_viewed' : 0})
            query_key = "pd" . desc_language_id
            products_description_queries[query_key] = {'type' : 'insert',
                'query' : products_description_query}

        if(products_description_queries):
            products_description_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : products_description_queries})


        # images
        if(convert['images']):
            url_image = self.getConnectorUrl('image')
            images_import_data = {}
            images = {}
            for key, image in convert['images'].items():
                image_process = self.processImageBeforeImport(image['url'], image['path'])
                image_key = 'i' + key
                images_import_data[image_key] = {'type' : 'download',
                    'path' : self.addPrefixPath(image_process['path'], self._notice['target']['config']['image_product']),
                    'params' : {'url' : image_process['url'],
                        'rename' : True}}
                images[key] = image

            if(images_import_data):
                image_import = self.getConnectorData(url_image, {'images' : images_import_data})
                if(image_import and image_import['result'] == 'success'):
                    products_images_queries = {}
                    sort_order = 0
                    for image_key, image in images.items():
                        if (self.iset(image_import['data'],image_key)):
                            image_import_path = image_import['data'][image_key]
                        else:
                            image_import_path = False

                        #image_import_path = isset(image_import['data'][image_key]) ? image_import['data'][image_key] : False
                        if(image_import_path):
                            image_import_path = self.removePrefixPath(image_import_path, self._notice['target']['config']['image_product'])
                            products_images_query = "INSERT INTO products_images "
                            products_images_query += self.arrayToInsertCondition({'products_id' : product_id,
                                'image' : image_import_path,
                                'htmlcontent' : image['label'],
                                'sort_order' : sort_order})
                            sort_order = sort_order + 1
                            products_images_queries[image_key] = {'type' : 'insert',
                                'query' : products_images_query}
                        
                    if(products_images_queries):
                        products_images_import = self.getConnectorData(url_query, {'serialize' : False,
                            'query' : products_images_queries})


        # categories
        if(convert['categories']):
            category_queries = {}
            for key, category in  convert['categories'].items():
                category_id = self.getMapFieldBySource(self.TYPE_CATEGORY, category['id'], category['code'])
                if(category_id):
                    category_query = "INSERT INTO products_to_categories "
                    category_query = category_query +  self.arrayToInsertCondition({'products_id' : product_id,
                        'categories_id' : category_id})
                    query_key = "ptc" + key
                    category_queries[query_key] = {'type' : 'insert',
                        'query' : category_query}

            if(category_queries):
                product_category_import = self.getConnectorData(url_query, {'serialize' : False,
                    'query' : category_queries})


        # attributes
        if(convert['options']):
            for option in convert['options'].items():
                option_id = False
                if(option['id'] or option['code']):
                    option_id = self.getMapFieldBySource(self.TYPE_ATTR, option['id'], option['code'])

                if(option_id == False):
                    option_next_id_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                            'query' : "SELECT MAX(products_options_id) + 1 AS next_id FROM products_options"}})
                    if(option_next_id_data == False or option_next_id_data['result'] != 'success'):
                        continue

                    option_id = option_next_id_data['data'][0]['next_id']
                    if(option_id == False):
                        continue
                    
                    option_queries = {}
                    for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                        if (self.iset(option['option_languages'],src_language_id)):
                            optionLang  = option['option_languages'][src_language_id]
                        else:
                            optionLang = option
                        #optionLang = isset(option['option_languages'][src_language_id]) ? option['option_languages'][src_language_id] : option
                        option_query = "INSERT INTO products_options "
                        option_query += self.arrayToInsertCondition({'products_options_id' : option_id,
                            'language_id' : desc_language_id,
                            'products_options_name' : optionLang['option_name']})
                        query_key = 'po' . desc_language_id
                        option_queries[query_key] = {'type' : 'insert',
                            'query' : option_query}

                    if(option_queries == False):
                        continue

                    option_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : option_queries})
                    if(option_import == False or option_import['result'] != 'success'):
                        continue
                    
                    if(option['id'] or option['code']):
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc) values('%s','%s','%s',%d,%d)""" %(url_src, url_desc, self.TYPE_ATTR, option['id'], option_id, option['code'])
                        self.insertdb(conn,query)
                        #self.insertMap(url_src, url_desc, self.TYPE_ATTR, option['id'], option_id, option['code'])
                    

                joinQueries = {}
                for option_value in option['values'].items():
                    option_value_id = False
                    if(option_value['id'] or option_value['code']):
                        option_value_id = self.getMapFieldBySource(self.TYPE_ATTR_VALUE, option_value['id'], option_value['code'])
                    
                    if(option_value_id):
                        product_attribute_join_query = "INSERT INTO products_attributes "
                        product_attribute_join_query += self.arrayToInsertCondition({'products_id' : product_id,
                            'options_id' : option_id,
                            'options_values_id' : option_value_id,
                            'options_values_price' : option_value['price'],
                            'price_prefix' : option_value['price_prefix']})
                        joinQueries['pa' . option_value_id] = {'type' : 'insert',
                            'query' : product_attribute_join_query}
                        continue
                    
                    option_value_next_id_data = self.getConnectorData(url_query, {'type' : 'select',
                        'query' : "SELECT MAX(products_options_values_id) + 1 AS next_id FROM products_options_values",})
                    if(option_value_next_id_data == False or option_value_next_id_data['result'] != 'success'):
                        continue
                    
                    option_value_id = option_value_next_id_data['data'][0]['next_id']
                    if(option_value_id == False):
                        continue
                    
                    option_value_queries = {}
                    for src_language_id, desc_language_id in self._notice['map']['languages'].items():
                        if (self.iset(option_value['option_value_languages'],src_language_id)):
                            optionLang = option_value['option_value_languages'][src_language_id]
                        else:
                            optionLang = option_value
                        #optionValueLang = isset(option_value['option_value_languages'][src_language_id]) ? option_value['option_value_languages'][src_language_id] : option_value
                        option_value_query = "INSERT INTO products_options_values "
                        option_value_query += self.arrayToInsertCondition({'products_options_values_id' : option_value_id,
                            'language_id' : desc_language_id,
                            'products_options_values_name' : optionValueLang['option_value_name']})
                        query_key = 'pov' . desc_language_id
                        option_value_queries[query_key] = {'type' : 'insert',
                            'query' : option_value_query}
                    
                    if(option_value_queries == False):
                        continue
                    option_value_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : option_value_queries})
                    if(option_value_import == False or option_value_import['result'] != 'success'):
                        continue

                    if(option_value['id'] or option_value['code']):
                        #self.insertMap(url_src, url_desc, self.TYPE_ATTR_VALUE, option_value['code'], option_value_id, option_value['code'])
                        conn = self.connectdb()
                        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc) values('%s','%s','%s',%d,%d)""" %(url_src, url_desc, self.TYPE_ATTR_VALUE, option_value['code'], option_value_id, option_value['code'])
                        self.insertdb(conn,query)

                    option_value_join_query = "INSERT INTO products_options_values_to_products_options "
                    option_value_join_query += self.arrayToInsertCondition({'products_options_id' : option_id,
                        'products_options_values_id' : option_value_id})
                    joinQueries['povtpo' . option_value_id] = {'type' : 'insert',
                        'query' : option_value_join_query}

                    product_attribute_join_query = "INSERT INTO products_attributes "
                    product_attribute_join_query += self.arrayToInsertCondition({'products_id' : product_id,
                        'options_id' : option_id,
                        'options_values_id' : option_value_id,
                        'options_values_price' : option_value['price'],
                        'price_prefix' : option_value['price_prefix']})
                    joinQueries['pa' . option_value_id] = {'type' : 'insert',
                        'query' : product_attribute_join_query}
                
                if(joinQueries):
                    joinImport = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : joinQueries})
                
            
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    

    def additionProductImport(self,product_id, convert, product, productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    

    #

    
        
    

















    '''def constructProducts(self):
        return {'quantity':'',
        'model':'',
        'image':''}


    def convertProductExport(self,products,i):
        products_data = self.constructProducts()
        products_data['quantity'] = products['data'][i]['products_quantity']
        products_data['model'] = products['data'][i]['products_model']
        products_data['image'] = products['data'][i]['products_image']
        return products_data'''




    #customer

    def getCustomersMainExport(self):
        last_id_f = self.get_id_last('Customer')
        query = "SELECT * FROM customers where customers_id >%d and customers_id < %d"  %(last_id_f,last_id_f+12)
        query = {'query':{'type':'select', 'query':query},}
        url = self.notice['src']['cart_url'] + 'catalog/cartmigration_connector/connector.php?action=query&token=' + self.notice['src']['config']['token']
        customers = self.getConnectorData(url, query)
        #print(customers)
        return customers



    def getCustomersExtExport(self,customers):
        query_url = self.getConnectorUrl('query')
        customerId = self.duplicateFieldValueFromList(customers['data'], 'customers_id')
        customer_id_con = self.arrayToInCondition(customerId)
        customer_ext_queries = {'customers_info' : {'type' : 'select',
                'query' : "SELECT * FROM customers_info WHERE customers_info_id IN " + customer_id_con},
            'address_book' : {'type' : 'select',
                'query' : "SELECT * FROM address_book WHERE customers_id IN " + customer_id_con}}
        # add custom
        customersExt = self.getConnectorData(query_url, {'serialize' : False,
            'query' : customer_ext_queries})
        if(customersExt ==False or customersExt['result'] != 'success'):
            return self.errorConnector()

        countryId = self.duplicateFieldValueFromList(customersExt['data']['address_book'], 'entry_country_id')
        stateId = self.duplicateFieldValueFromList(customersExt['data']['address_book'], 'entry_zone_id')
        country_id_con = self.arrayToInCondition(countryId)
        state_id_con = self.arrayToInCondition(stateId)
        customer_ext_rel_queries = {'countries' : {'type' : 'select',
                'query' : "SELECT * FROM countries WHERE countries_id IN " + country_id_con},
            'zones' : {'type' : 'select',
                'query' : "SELECT * FROM zones WHERE zone_id IN " . state_id_con}}
        # add custom
        customersExtRel = self.getConnectorData(query_url, {'serialize' : False,
            'query' : customer_ext_rel_queries})
        if(customersExtRel == False or customersExtRel['result'] != 'success'):
            return self.errorConnector()
        
        customersExt = self.syncConnectorObject(customersExt, customersExtRel)
        return customersExt
    





    def convertCustomerExport(self,customer, customersExt):
        #var_dump(customersExt)exit()
        customer_data = self.constructCustomer()
        customer_data = self.addConstructDefault(customer_data)
        customer_data['id'] = customer['customers_id']
        customer_data['username'] = customer['customers_email_address']
        customer_data['email'] = customer['customers_email_address']
        customer_data['password'] = customer['customers_password']
        customer_data['first_name'] = customer['customers_firstname']
        customer_data['last_name'] = customer['customers_lastname']
        customer_data['gender'] = customer['customers_gender']
        customer_data['dob'] = customer['customers_dob']
        customer_data['is_subscribed'] = customer['customers_newsletter']
        customer_data['active'] = True

        customer_info = self.getRowFromListByField(customersExt['data']['customers_info'], 'customers_info_id', customer['customers_id'])
        if(customer_info):
            customer_data['created_at'] = customer_info['customers_info_date_account_created']
            customer_data['updated_at'] = customer_info['customers_info_date_account_last_modified']
        

        addressBook = self.getListFromListByField(customersExt['data']['address_book'], 'customers_id', customer['customers_id'])
        if(addressBook):
            for address_book in addressBook.items():
                address_data = self.constructCustomerAddress()
                address_data = self.addConstructDefault(address_data)
                address_data['id'] = address_book['address_book_id']
                address_data['first_name'] = address_book['entry_firstname']
                address_data['last_name'] = address_book['entry_lastname']
                address_data['gender'] = address_book['entry_gender']
                address_data['address_1'] = address_book['entry_street_address']
                address_data['address_2'] = address_book['entry_suburb']
                address_data['city'] = address_book['entry_city']
                address_data['postcode'] = address_book['entry_postcode']
                address_data['telephone'] = customer['customers_telephone']
                address_data['company'] = address_book['entry_company']
                address_data['fax'] = customer['customers_fax']

                country = self.getRowFromListByField(customersExt['data']['countries'], 'countries_id', address_book['entry_country_id'])
                if(country):
                    address_data['country']['id'] = country['countries_id']
                    address_data['country']['country_code'] = country['countries_iso_code_2']
                    address_data['country']['name'] = country['countries_name']
                else:
                    address_data['country']['country_code'] = 'US'
                    address_data['country']['name'] = 'United States'
                

                state_id = address_book['entry_zone_id']
                if(state_id):
                    state = self.getRowFromListByField(customersExt['data']['zones'], 'zone_id', state_id)
                    if(state):
                        address_data['state']['id'] = state['zone_id']
                        address_data['state']['state_code'] = state['zone_code']
                        address_data['state']['name'] = state['zone_name']
                    else:
                        address_data['state']['state_code'] = 'AL'
                        address_data['state']['name'] = 'Alabama'
                    
                else:
                    address_data['state']['name'] = address_book['entry_state']
                
                if(address_book['address_book_id'] == customer['customers_default_address_id']):
                    address_data['default']['billing'] = True
                    address_data['default']['shipping'] = True
                
                customer_data['address'].append(address_data)
            
        
        return {'result' : 'success',
            'msg' : '',
            'data' : customer_data}
    
    def  getCustomerIdImport(self,convert, customer, customersExt):
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
        
        if(convert['middle_name']):
            middle_name = convert['middle_name'] + convert['last_name']
        else:
            middle_name = convert['last_name']
        customer_data = {'customers_gender' : self.oscConvertGender(convert['gender']),
            'customers_firstname' : convert['first_name'],
            'customers_lastname' : middle_name, #(convert['middle_name']) ? convert['middle_name'] . ' ' . convert['last_name'] : convert['last_name'],
            'customers_dob' : convert['dob'],
            'customers_email_address' : convert['email'],
            'customers_password' : convert['password'],
            'customers_newsletter' : convert['is_subscribed']}
        if(self._notice['config']['pre_cus']):
            delete_customer = self.deleteTargetCustomer(convert['id'])
            customer_data['customers_id'] = convert['id']
        
        customer_query = "INSERT INTO customers "
        customer_query += self.arrayToInsertCondition(customer_data)

        customer_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : customer_query,
                'params' : {'insert_id' : True}}})

        if(customer_import == False or customer_import['result'] != 'success'):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        customer_id = customer_import['data']
        if(customer_id == False):
            response['result'] = 'warning'
            response['msg'] =  'warning' #self.warningImportEntity('Customer', convert['id'], convert['code'])
            return response
        

        #self.insertMap(url_src, url_desc, self.TYPE_CUSTOMER, convert['id'], customer_id, convert['code'])
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_CUSTOMER, convert['id'], customer_id, convert['code'])
        self.insertdb(conn,query)        
        return {'result' : 'success',
            'msg' : '',
            'data' : customer_id}
        
    



    def additionCustomerImport(customer_id, convert, customer, customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}




    # order

    def prepareOrdersImport(self):
        #parent = parent.prepareOrdersImport()
        url_query = self.getConnectorUrl('query')
        data = self.getConnectorData(url_query, {'serialize' : False,
            'query' : {'currencies' : {'type' : 'select',
                    'query' : "SELECT * FROM currencies"}}})
        if(data == False or data['result'] != 'success'):
            return self
        
        currencies = data['data']['currencies']
        if(currencies == False):
            currencies = self.oscDefaultCurrency()
        
        self._notice['target']['extends']['currencies'] = currencies
        return self
    

    def getOrdersMainExport(self):
        last_id_f = self.get_id_last()
        #id_src = self._notice['process']['orders']['id_src']
        limit = self._notice['setting']['orders']
        last_id_f = self.get_id_last('Order')
        orders = self.getConnectorData(self.getConnectorUrl('query'), {'query' : {'type' : 'select',
                'query' : "SELECT * FROM orders WHERE orders_id > " + last_id_f + " ORDER BY orders_id ASC LIMIT " + limit}})
        if(orders  == None or orders['result'] != 'success'):
            return self.errorConnector()
        return orders
    

    def getOrdersExtExport(self,orders):
        url_query = self.getConnectorUrl('query')
        orderId = self.duplicateFieldValueFromList(orders['data'], 'orders_id')
        currencyCode = self.duplicateFieldValueFromList(orders['data'], 'currency')
        bilCountry = {self.duplicateFieldValueFromList(orders['data'], 'billing_country')}
        delCountry = {self.duplicateFieldValueFromList(orders['data'], 'delivery_country')}
        cusCountry = {self.duplicateFieldValueFromList(orders['data'], 'customers_country')}
        countries = bilCountry + delCountry
        countries = list(set(countries + cusCountry))
        bilState = self.duplicateFieldValueFromList(orders['data'], 'billing_state')
        delState = self.duplicateFieldValueFromList(orders['data'], 'delivery_state')
        cusState = self.duplicateFieldValueFromList(orders['data'], 'customers_state')
        states = bilState + delState
        states = list(set(states + cusState))
        order_id_con = self.arrayToInCondition(orderId)
        currency_con = self.arrayToInCondition(currencyCode)
        countries_con = self.arrayToInCondition(countries, False)
        states_con = self.arrayToInCondition(states, False)
        orders_ext_queries = {'orders_total' : {'type' : 'select',
                'query' : "SELECT * FROM orders_total WHERE orders_id IN " + order_id_con},
            'orders_products' : {'type' : 'select',
                'query' : "SELECT * FROM orders_products WHERE orders_id IN " + order_id_con},
            'orders_products_attributes' : {'type' : 'select',
                'query' : "SELECT * FROM orders_products_attributes WHERE orders_id IN " + order_id_con},
            'orders_status_history' : {'type' : 'select',
                'query' : "SELECT * FROM orders_status_history WHERE orders_id IN " + order_id_con + " ORDER BY orders_status_history_id ASC"},
            'currencies' : {'type' : 'select',
                'query' : "SELECT * FROM currencies WHERE code IN " + currency_con},
            'countries' : {'type' : 'select',
                'query' : "SELECT * FROM countries WHERE countries_name IN " + countries_con},
            'zones' : {'type' : 'select',
                'query' : "SELECT * FROM zones WHERE zone_id IN " + states_con + " OR zone_name IN " + states_con}}
        # add custom
        ordersExt = self.getConnectorData(url_query, {'serialize' : False,
            'query' : orders_ext_queries})
        if(ordersExt == False or ordersExt['result'] != 'success'):
            return self.errorConnector()
        
        orders_ext_rel_queries = {}
        # add custom
        if(orders_ext_rel_queries):
            ordersExtRel = self.getConnectorData(url_query, {'serialize' : False,
                'query' : orders_ext_rel_queries})
            if(ordersExtRel == False or ordersExtRel['result'] != 'success'):
                return self.errorConnector()
            
            ordersExt = self.syncConnectorObject(ordersExt, ordersExtRel)
        
        return ordersExt
    

    def convertOrderExport(order, ordersExt):
        order_data = self.constructOrder()
        order_data = self.addConstructDefault(order_data)
        order_data['id'] = order['orders_id']
        order_data['status'] = order['orders_status']

        orderTotal = self.getListFromListByField(ordersExt['data']['orders_total'], 'orders_id', order['orders_id'])
        otTax = self.getRowFromListByField(orderTotal, 'class', 'ot_tax')
        otShipping = self.getRowFromListByField(orderTotal, 'class', 'ot_shipping')
        otSubtotal = self.getRowFromListByField(orderTotal, 'class', 'ot_subtotal')
        otTotal = self.getRowFromListByField(orderTotal, 'class', 'ot_total')
        if(otTax):
            order_data['tax']['title'] = otTax['title']
            order_data['tax']['amount'] = otTax['value']
            if(otSubtotal):
                order_data['tax']['percent'] = self.convertFloatToPercent(otTax['value'] / otSubtotal['value'])
            
        
        if(otShipping):
            order_data['shipping']['title'] = otShipping['title']
            order_data['shipping']['amount'] = otShipping['value']
        
        if(otSubtotal):
            order_data['subtotal']['title'] = otSubtotal['title']
            order_data['subtotal']['amount'] = otSubtotal['value']
        
        if(otTotal):
            order_data['total']['title'] = otTotal['title']
            order_data['total']['amount'] = otTotal['value']
        

        currency = self.getRowValueFromListByField(ordersExt['data']['currencies'], 'code', order['currency'], 'currencies_id')
        order_data['currency'] = currency

        order_data['created_at'] = order['date_purchased']
        order_data['updated_at'] = order['last_modified']

        order_customer = self.constructOrderCustomer()
        order_customer = self.addConstructDefault(order_customer)
        order_customer['id'] = order['customers_id']
        order_customer['email'] = order['customers_email_address']
        customer_name = self.getNameFromString(order['customers_name'])
        order_customer['first_name'] = customer_name['firstname']
        order_customer['last_name'] = customer_name['lastname']
        order_data['customer'] = order_customer

        customer_address = self.constructOrderAddress()
        customer_address = self.addConstructDefault(customer_address)
        customer_address['first_name'] = customer_name['firstname']
        customer_address['last_name'] = customer_name['lastname']
        customer_address['address_1'] = order['customers_street_address']
        customer_address['address_2'] = order['customers_suburb']
        customer_address['city'] = order['customers_city']
        customer_address['postcode'] = order['customers_postcode']
        customer_address['telephone'] = order['customers_telephone']
        customer_address['company'] = order['customers_company']
        customer_country = self.getRowFromListByField(ordersExt['data']['countries'], 'countries_name', order['customers_country'])
        if(customer_country):
            customer_address['country']['id'] = customer_country['countries_id']
            customer_address['country']['country_code'] = customer_country['countries_iso_code_2']
        
        customer_address['country']['name'] = order['customers_country']
        customer_state = self.getRowFromListByField(ordersExt['data']['zones'], 'zone_name', order['customers_state'])
        if(customer_state):
            customer_address['state']['id'] = customer_state['zone_id']
            customer_address['state']['state_code'] = customer_state['zone_code']
        
        customer_address['state']['name'] = order['customers_state']
        order_data['customer_address'] = customer_address

        order_billing = self.constructOrderAddress()
        order_billing = self.addConstructDefault(order_billing)
        billing_name = self.getNameFromString(order['billing_name'])
        order_billing['first_name'] = billing_name['firstname']
        order_billing['last_name'] = billing_name['lastname']
        order_billing['address_1'] = order['billing_street_address']
        order_billing['address_2'] = order['billing_suburb']
        order_billing['city'] = order['billing_city']
        order_billing['postcode'] = order['billing_postcode']
        order_billing['telephone'] = order['customers_telephone']
        order_billing['company'] = order['billing_company']
        billing_country = self.getRowFromListByField(ordersExt['data']['countries'], 'countries_name', order['billing_country'])
        if(billing_country):
            order_billing['country']['id'] = billing_country['countries_id']
            order_billing['country']['code'] = billing_country['countries_iso_code_2']
            order_billing['country']['country_code'] = billing_country['countries_iso_code_2']
        
        order_billing['country']['name'] = order['billing_country']
        billing_state = self.getRowFromListByField(ordersExt['data']['zones'], 'zone_name', order['billing_state'])
        if(billing_state):
            order_billing['state']['id'] = billing_state['zone_id']
            order_billing['state']['state_code'] = billing_state['zone_code']
        
        order_billing['state']['name'] = order['billing_state']
        order_data['billing_address'] = order_billing

        order_delivery = self.constructOrderAddress()
        order_delivery = self.addConstructDefault(order_delivery)
        delivery_name = self.getNameFromString(order['delivery_name'])
        order_delivery['first_name'] = delivery_name['firstname']
        order_delivery['last_name'] = delivery_name['lastname']
        order_delivery['address_1'] = order['delivery_street_address']
        order_delivery['address_2'] = order['delivery_suburb']
        order_delivery['city'] = order['delivery_city']
        order_delivery['postcode'] = order['delivery_postcode']
        order_delivery['telephone'] = order['customers_telephone']
        order_delivery['company'] = order['delivery_company']
        delivery_country = self.getRowFromListByField(ordersExt['data']['countries'], 'countries_name', order['delivery_country'])
        if(delivery_country):
            order_delivery['country']['id'] = delivery_country['countries_id']
            order_delivery['country']['code'] = delivery_country['countries_iso_code_2']
            order_delivery['country']['country_code'] = delivery_country['countries_iso_code_2']
        
        order_delivery['country']['name'] = order['delivery_country']
        delivery_state = self.getRowFromListByField(ordersExt['data']['zones'], 'zone_name', order['delivery_state'])
        if(delivery_state):
            order_delivery['state']['id'] = delivery_state['zone_id']
            order_delivery['state']['state_code'] = delivery_state['zone_code']
        
        order_delivery['state']['name'] = order['delivery_state']
        order_delivery = self._cookShippingAddressByBilling(order_delivery, order_billing)
        order_data['shipping_address'] = order_delivery

        order_payment = self.constructOrderPayment()
        order_payment = self.addConstructDefault(order_payment)
        order_payment['title'] = order['payment_method']
        order_data['payment'] = order_payment

        orderProduct = self.getListFromListByField(ordersExt['data']['orders_products'], 'orders_id', order['orders_id'])
        orderProductAttributes = self.getListFromListByField(ordersExt['data']['orders_products_attributes'], 'orders_id', order['orders_id'])
        orderItem = []
        for order_product in orderProduct.items():
            order_item_subtotal = order_product['final_price'] * order_product['products_quantity']
            order_item_tax = order_item_subtotal * order_product['products_tax']
            order_item_total = order_item_subtotal + order_item_tax
            order_item = self.constructOrderItem()
            order_item = self.addConstructDefault(order_item)
            order_item['id'] = order_product['orders_products_id']
            order_item['product']['id'] = order_product['products_id']
            order_item['product']['name'] = order_product['products_name']
            order_item['product']['sku'] = order_product['products_model']
            order_item['qty'] = order_product['products_quantity']
            order_item['price'] = order_product['final_price']
            order_item['original_price'] = order_product['products_price']
            order_item['tax_amount'] = order_item_tax
            order_item['tax_percent'] = order_product['products_tax']
            order_item['discount_amount'] = '0.0000'
            order_item['discount_percent'] = '0.0000'
            order_item['subtotal'] = order_item_subtotal
            order_item['total'] = order_item_total
            orderProductAttribute = self.getListFromListByField(orderProductAttributes, 'orders_products_id', order_product['orders_products_id'])
            if(orderProductAttribute):
                orderItemOption = []
                for  order_product_attribute in orderProductAttribute.items():
                    order_item_option = self.constructOrderItemOption()
                    order_item_option['option_name'] = order_product_attribute['products_options']
                    order_item_option['option_value_name'] = order_product_attribute['products_options_values']
                    order_item_option['price'] = order_product_attribute['options_values_price']
                    order_item_option['price_prefix'] = order_product_attribute['price_prefix']
                    orderItemOption.append(order_item_option)
                
                order_item['options'] = orderItemOption
            
            orderItem.append(order_item)
        
        order_data['items'] = orderItem

        orderStatusHistory = self.getListFromListByField(ordersExt['data']['orders_status_history'], 'orders_id', order['orders_id'])
        orderHistory = []
        for orders_status_history in orderStatusHistory.items():
            order_history = self.constructOrderHistory()
            order_history = self.addConstructDefault(order_history)
            order_history['id'] = orders_status_history['orders_status_history_id']
            order_history['status'] = orders_status_history['orders_status_id']
            order_history['comment'] = orders_status_history['comments']
            order_history['notified'] = orders_status_history['customer_notified']
            order_history['created_at'] = orders_status_history['date_added']
            orderHistory.append(order_history)
        
        order_data['histories'] = orderHistory

        return {'result' : 'success',
            'msg' : '',
            'data' : order_data}
    

    def getOrderIdImport(self,convert, order, ordersExt):
        return convert['id']
    

    def checkOrderImport(self,convert, order, ordersExt):
        if (self.getMapFieldBySource(self.TYPE_ORDER, convert['id'], convert['code'])):
            return True
        else:
            return False
        #return self.getMapFieldBySource(self.TYPE_ORDER, convert['id'], convert['code']) ? True : False
    

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
        if(customer_id == False):
            customer_id = 0
        if(self.iset(self._notice['map']['order_status'],convert['status'])):
            order_status = self._notice['map']['order_status'][convert['status']]
        else:
            order_status = 1
        #order_status = isset(self._notice['map']['order_status'][convert['status']]) ? self._notice['map']['order_status'][convert['status']] : 1
        #currency_id = isset(self._notice['currencies'][convert['currency']]) ? self._notice['currencies'][convert['currency']] : 1
        if(self.iset(self._notice['currencies'],convert['currency'])):
            currency_id = self._notice['currencies'][convert['currency']]
        else:
            currency_id = 1
        currency = self.getRowFromListByField(self._notice['target']['extends']['currencies'], 'currencies_id', currency_id)
        if(currency):
            currency_code = currency['code']
            currency_value = currency['value']
        else:
            currency_code = 'USD'
            currency_value = '1.000000'
        
        order_data = {'customers_id' : customer_id,
            'customers_name' : self.createNameFromParts(customer['first_name'], customer['last_name'], customer['middle_name']),
            'customers_company' : customer_address['company'],
            'customers_street_address' : customer_address['address_1'],
            'customers_suburb' : customer_address['address_2'],
            'customers_city' : customer_address['city'],
            'customers_postcode' : customer_address['postcode'],
            'customers_state' : customer_address['state']['name'],
            'customers_country' : customer_address['country']['name'],
            'customers_telephone' : customer_address['telephone'],
            'customers_email_address' : customer['email'],
            'customers_address_format_id' : 1,
            'delivery_name' : self.createNameFromParts(shipping_address['first_name'], shipping_address['last_name'], shipping_address['middle_name']),
            'delivery_company' : shipping_address['company'],
            'delivery_street_address' : shipping_address['address_1'],
            'delivery_suburb' : shipping_address['address_2'],
            'delivery_city' : shipping_address['city'],
            'delivery_postcode' : shipping_address['postcode'],
            'delivery_state' : shipping_address['state']['name'],
            'delivery_country' : shipping_address['country']['name'],
            'delivery_address_format_id' : 1,
            'billing_name' : self.createNameFromParts(billing_address['first_name'], billing_address['last_name'], billing_address['middle_name']),
            'billing_company' : billing_address['company'],
            'billing_street_address' : billing_address['address_1'],
            'billing_suburb' : billing_address['address_2'],
            'billing_city' : billing_address['city'],
            'billing_postcode' : billing_address['postcode'],
            'billing_state' : billing_address['state']['name'],
            'billing_country' : billing_address['country']['name'],
            'billing_address_format_id' : 1,
            'payment_method' : convert['payment']['title'],
            'last_modified' : convert['updated_at'],
            'date_purchased' : convert['created_at'],
            'orders_status' : order_status,
            'currency' : currency_code,
            'currency_value' : currency_value}
        if(self._notice['config']['pre_ord']):
            order_delete = self.deleteTargetOrder(convert['id'])
            order_data['orders_id'] = convert['id']
        
        order_query = "INSERT INTO orders "
        order_query += self.arrayToInsertCondition(order_data)

        order_import = self.getConnectorData(url_query, {'query' : {'type' : 'insert',
                'query' : order_query,
                'params' : {'insert_id' : True}}})

        if(order_import == False or order_import['result'] != 'success'):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        order_id = order_import['data']
        if(order_id == False):
            response['result'] = 'warning'
            response['msg'] = 'warning'
            return response
        
        #self.insertMap(url_src, url_desc, self.TYPE_ORDER, convert['id'], order_id, convert['code'])
        conn = self.connectdb()
        query = """insert into cartmigration_map(url_src,url_desc,type,id_src,id_desc,code) values('%s','%s','%s',%d,%d,'%s')""" %(url_src, url_desc, self.TYPE_ORDER, convert['id'], order_id, convert['code'])
        self.insertdb(conn,query)
        return {'result' : 'success',
            'msg' : '',
            'data' : order_id}
        
    

    def afterOrderImport(self,order_id, convert, order, ordersExt):
        url_query = self.getConnectorUrl('query')

        ## order total
        orders_total_queries = {}
        if(self.iset(self._notice['currencies'],convert['currency'])):
            currency_id = self._notice['currencies'][convert['currency']]
        else:
            currency_id = 1
        #currency_id = isset(self._notice['currencies'][convert['currency']]) ? self._notice['currencies'][convert['currency']] : 1
        currency = self.getRowFromListByField(self._notice['target']['extends']['currencies'], 'currencies_id', currency_id)
        ot_sort_order = 1
        otTax = convert['tax']
        if(otTax['amount']):
            if(otTax['title']):
                ot_tax_title = self.oscOrderTitleFormat(otTax['title'])
            else:
                ot_tax_title = 'Tax'
            #ot_tax_title = otTax['title'] ? self.oscOrderTitleFormat(otTax['title']) : 'Tax:'
            ot_tax_value = otTax['amount']
            ot_tax_text = self.oscCurrencyFormat(currency, ot_tax_value)
            ot_tax_query = "INSERT INTO orders_total "
            ot_tax_query += self.arrayToInsertCondition({'orders_id' : order_id,
                'title' : ot_tax_title,
                'text' : ot_tax_text,
                'value' : ot_tax_value,
                'class' : 'ot_tax',
                'sort_order' : ot_sort_order})
            ot_sort_order = ot_sort_order + 1
            orders_total_queries['ot_tax'] = {'type' : 'insert',
                'query' : ot_tax_query}
        

        otShipping = convert['shipping']
        if(otShipping['amount']):
            ot_shipping_title = self.oscOrderTitleFormat(otShipping['title']) if otShipping['title'] else 'Shipping'
            #ot_shipping_title = otShipping['title'] ? self.oscOrderTitleFormat(otShipping['title']) : 'Shipping:'
            ot_shipping_value = otShipping['amount']
            ot_shipping_text = self.oscCurrencyFormat(currency, ot_shipping_value)
            ot_shipping_query = "INSERT INTO orders_total "
            ot_shipping_query = ot_shipping_query + self.arrayToInsertCondition({'orders_id' : order_id,
                'title' : ot_shipping_title,
                'text' : ot_shipping_text,
                'value' : ot_shipping_value,
                'class' : 'ot_shipping',
                'sort_order' : ot_sort_order})
            ot_sort_order = ot_sort_order + 1
            orders_total_queries['ot_shipping'] = {'type' : 'insert',
                'query' : ot_shipping_query}
        

        otSubtotal = convert['subtotal']
        if(otSubtotal['amount']):
            ot_subtotal_title = self.oscOrderTitleFormat(otSubtotal['title']) if otSubtotal['title'] else 'Sub-Total:'
            #ot_subtotal_title = otSubtotal['title'] ? self.oscOrderTitleFormat(otSubtotal['title']) : 'Sub-Total:'
            ot_subtotal_value = otSubtotal['amount']
            ot_subtotal_text = self.oscCurrencyFormat(currency, ot_subtotal_value)
            ot_subtotal_query = "INSERT INTO orders_total "
            ot_subtotal_query += self.arrayToInsertCondition({'orders_id' : order_id,
                'title' : ot_subtotal_title,
                'text' : ot_subtotal_text,
                'value' : ot_subtotal_value,
                'class' : 'ot_subtotal',
                'sort_order' : ot_sort_order})
            ot_sort_order = ot_sort_order + 1
            orders_total_queries['ot_subtotal'] = {'type' : 'insert',
                'query' : ot_subtotal_query}
        

        otTotal = convert['total']
        if(otTotal['amount']):
            ot_total_title = self.oscOrderTitleFormat(otTotal['title']) if otTotal['title'] else 'Total:'
            #ot_total_title = otTotal['title'] ? self.oscOrderTitleFormat(otTotal['title']) : 'Total:'
            ot_total_value = otTotal['amount']
            ot_total_text = '<strong>' + self.oscCurrencyFormat(currency, ot_total_value) + '</strong>'
            ot_total_query = "INSERT INTO orders_total "
            ot_total_query += self.arrayToInsertCondition({'orders_id' : order_id,
                'title' : ot_total_title,
                'text' : ot_total_text,
                'value' : ot_total_value,
                'class' : 'ot_total',
                'sort_order' : ot_sort_order})
            orders_total_queries['ot_total'] = {'type' : 'insert',
                'query' : ot_total_query}
        
        if(orders_total_queries):
            orders_total_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : orders_total_queries})
        

        # order product
        items = convert['items']
        order_product_queries = []
        order_product_attributes = []
        for  key, item in items.items():
            product_id = self.getMapFieldBySource(self.TYPE_PRODUCT, item['product']['id'], item['product']['code'])
            if(product_id == False):
                product_id = 0
            
            item_query = "INSERT INTO orders_products "
            item_query += self.arrayToInsertCondition({'orders_id' : order_id,
                'products_id' : product_id,
                'products_model' : item['product']['sku'],
                'products_name' : item['product']['name'],
                'products_price' : item['original_price'],
                'final_price' : item['price'],
                'products_tax' : item['tax_percent'],
                'products_quantity' : item['qty']})
            query_key = 'op' + key
            order_product_queries[query_key] = {'type' : 'insert',
                'query' : item_query,
                'params' : {'insert_id' : True}}
            if(item['options']):
                for  option in item['options'].items():
                    option_data = {'orders_id' : order_id,
                        'orders_products_id' : query_key,
                        'products_options' : option['option_name'],
                        'products_options_values' : option['option_value_name'],
                        'options_values_price' : option['price'],
                        'price_prefix' : option['price_prefix']}
                    order_product_attributes.append(option_data)
                
        if(order_product_queries):
            order_product_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : order_product_queries})
            if(order_product_import and order_product_import['result'] == 'success'):
                order_product_attribute_queries = {}
                for  key, order_product_attribute in order_product_attributes.items():
                    query_key = order_product_attribute['orders_products_id']
                    order_product_id = order_product_import['data'][query_key] if(self.iset(order_product_import['data'],query_key)) else False
                    #order_product_id = isset(order_product_import['data'][query_key]) ? order_product_import['data'][query_key] : False
                    if(order_product_id == False):
                        continue
                    
                    order_product_attribute['orders_products_id'] = order_product_id
                    order_product_attribute_query = "INSERT INTO orders_products_attributes "
                    order_product_attribute_query += self.arrayToInsertCondition(order_product_attribute)
                    order_product_attribute_queries['opa' . key] = {'type' : 'insert',
                        'query' : order_product_attribute_query}
                
                if(order_product_attribute_queries):
                    order_product_attribute_import = self.getConnectorData(url_query, {'serialize' : False,
                        'query' : order_product_attribute_queries})
                
            
        

        # order history
        histories = convert['histories']
        history_queries = {}
        for  key, history in histories.items():
            order_status_id = self._notice['map']['order_status'][history['status']] if (self.iset(self._notice['map']['order_status'],history['status'])) else 1
            #orders_status_id = isset(self._notice['map']['order_status'][history['status']]) ? self._notice['map']['order_status'][history['status']] : 1
            history_data = {'orders_id' : order_id,
                'orders_status_id' : orders_status_id,
                'date_added' : history['created_at'],
                'customer_notified' : history['notified'],
                'comments' : history['comment']}
            history_data = self.oscDatetimeRequire(history_data, 'date_added')
            history_query = "INSERT INTO orders_status_history "
            history_query = history_query +  self.arrayToInsertCondition(history_data)
            query_key = 'osh' + key
            history_queries[query_key] = {'type' : 'insert',
                'query' : history_query}
        
        if(history_queries):
            history_import = self.getConnectorData(url_query, {'serialize' : False,
                'query' : history_queries})
        

        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    

    def additionOrderImport(order_id, convert, order, ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}
        
    



        








    
    def addConstructDefault(self,construct):
        construct['site_id'] = 1
        construct['language_id'] = self._notice['src']['language_default']
        return construct


    def oscDatetimeRequire(self,data, fields = None):
        if(fields == False):
            return data
        datetime_now = self.datetimeNow()
        if(isinstance(fields, str)):
            if(data[fields] == False):
                data[fields] = datetimeNow()

        if(isinstance(fields, (frozenset, list, set, tuple))):
            for field in fields.items():
                if(data[field] == False):
                    data[field] = datetime_now
        return data



    def getCategoryParent(self,parent_id):
        response = self._defaultResponse()
        url_query = self.getConnectorUrl('query')
        categories = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                'query' : "SELECT * FROM categories WHERE categories_id = " + parent_id}})
        if(categories == False or categories['result'] != 'success'):
            response['result'] = 'warning'
            return response
        categoriesExt = self.getCategoriesExtExport(categories)
        if(categoriesExt == False or categoriesExt['result'] != "success"):
            response['result'] = 'warning'
            return response

        category = categories['data'][0]
        return self.convertCategoryExport(category, categoriesExt)

    def _importCategoryParent(self,parent):
        response = self._defaultResponse()
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        #parent_exists = self.selectMap(url_src, url_desc, self.TYPE_CATEGORY, parent['id'], None, parent['code'])
        query = """select * from category where url_src = '%s' and url_desc = '%s' and type = 'category' and id_src = %d and id_desc = %d and code_src = '%s'""" %(url_src, url_desc, self.TYPE_CATEGORY, parent['id'], None, parent['code'])
        conn = self.connectdb()
        parent_exists = self.selectdb(conn,query)
        if(parent_exists):
            response['result'] = 'success'
            response['data'] = parent_exists['id_desc']
            return response

        category = parent['category']
        categoriesExt = parent['categoriesExt']
        parent_import = self.categoryImport(parent, category, categoriesExt)
        if(parent_import['result'] != 'success'):
            return parent_import

        parent_id = parent_import['data']
        if(parent_id == False):
            response['result'] = 'warning'
            return response

        self.afterCategoryImport(parent_id, parent, category, categoriesExt)
        return parent_import



    def oscConvertGender(self,gender):
        result = self.GENDER_MALE
        if(gender == self.GENDER_FEMALE):
            result = self.GENDER_FEMALE
        
        return result



    def deleteTargetCustomer(self,customer_id):
        if(customer_id == False):
            return True
        
        delete = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : False,
            'query' : {'address_book' : {'type' : 'query',
                    'query' : "DELETE FROM address_book WHERE customers_id = " + customer_id},
                'customers_basket_attributes' : {'type' : 'query',
                    'query' : "DELETE FROM customers_basket_attributes WHERE customers_id = " + customer_id},
                'customers_basket' : {'type' : 'query',
                    'query' : "DELETE FROM customers_basket WHERE customers_id = " + customer_id},
                'customers_info' : {'type' : 'query',
                    'query' : "DELETE FROM customers_info WHERE customers_id = " + customer_id},
                'customers' : {'type' : 'query',
                    'query' : "DELETE FROM customers WHERE customers_id = " + customer_id}}})
        return True


    def oscDefaultCurrency(self):
        return {{'currencies_id' : 1,
                'title' : 'U.S. Dollar',
                'code' : 'USD',
                'symbol_left' : '',
                'symbol_right' : ' ',
                'decimal_point' : '.',
                'thousands_point' : ',',
                'decimal_places' : 2,
                'value' : '1.000000',
                'last_updated' : None}}


    def oscOrderTitleFormat(self,title):
        return title + ':'
    

    def oscCurrencyFormat(self,currency, number):
        format_string = currency['symbol_left']
        format_string = format_string + number_format(self.oscTepRound(number * currency['value'], currency['decimal_places']), currency['decimal_places'], currency['decimal_point'], currency['thousands_point'])
        format_string = format_string +  currency['symbol_right']
        return format_string



    def oscTepRound(number, precision):
        if (number.find('.') != -1 and (len(number[(number.find('.')+1):]) > precision)):
            number = number[0:(number.find('.') + 1 + precision + 1)]
            if (number[-1:] >= 5):
                if (precision > 1):
                    number = number[0:-1] + ('0.' + '0'*3 + '1')
                elif (precision == 1):
                    number = number[0:-1] + 0.1
                else:
                    number = number[0:-1] + 1
                
            else:
                number = number[0:-1]
            
        return number




    def getCountriesTarget(self):

        countries = []
        url_query = self.getConnectorUrl('query')
        country_count = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                'query' : "SELECT COUNT(1) AS count FROM countries"}})
            
        
        if(country_count == False or country_count['result'] != 'success'):
            return countries
        
        count = self.arrayToCount(country_count['data'], 'count')
        if(count == False):
            return countries
        
        id_src = 0
        per_batch = 30
        time = math.ceil(count/ per_batch)
        for i in  range(time):
            country_data = self.getConnectorData(url_query, {'query' : {'type' : 'select',
                    'query' : "SELECT countries_id,countries_name  FROM countries WHERE countries_id > " + id_src + " ORDER BY countries_id ASC LIMIT " +  per_batch}})
                
            
            if(country_data == False or country_data['result'] != 'success'):
                return {}
            
            for country in country_data['data'].items():
                countries.append(country)
                id_src = country['countries_id']
            
        
        return countries
    

os = Oscommerce()
os.getCustomersMainExport()


    
    

    
            
    




    

