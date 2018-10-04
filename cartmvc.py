import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading

from time import gmtime, strftime

class Cartmvc:
   #'Common base class for all employees'
 



#This would create first object of Employee class"
    global _type
    global _notice
    global _cart_url
    global _db

    CONNECTOR_SUFFIX  = '/cartmigration_connector/connector.php'
    TYPE_TAX          = 'tax'
    TYPE_TAX_PRODUCT  = 'tax_product'
    TYPE_TAX_CUSTOMER = 'tax_customer'
    TYPE_TAX_ZONE     = 'tax_zone'
    TYPE_TAX_ZONE_COUNTRY = 'tax_zone_country'
    TYPE_TAX_ZONE_STATE = 'tax_zone_state'
    TYPE_TAX_ZONE_RATE = 'tax_zone_rate'
    TYPE_MANUFACTURER = 'manufacturer'
    TYPE_CATEGORY     = 'category'
    TYPE_PRODUCT      = 'product'
    TYPE_CHILD        = 'product_child'
    TYPE_ATTR         = 'attr'
    TYPE_ATTR_VALUE   = 'attr_value'
    TYPE_OPTION       = 'option'
    TYPE_OPTION_VALUE = 'option_value'
    TYPE_CUSTOMER     = 'customer'
    TYPE_ADDRESS      = 'address'
    TYPE_ORDER        = 'order'
    TYPE_REVIEW       = 'review'
    TYPE_SHIPPING     = 'shipping'
    TYPE_PAGE         = 'page'
    TYPE_POST         = 'post'
    TYPE_FORMAT       = 'format'
    TYPE_COMMENT      = 'comment'
    TYPE_TAG          = 'tag'

    PRODUCT_SIMPLE    = 'simple'
    PRODUCT_CONFIG    = 'configurable'
    PRODUCT_VIRTUAL   = 'virtual'
    PRODUCT_DOWNLOAD  = 'download'
    PRODUCT_GROUP     = 'grouped'
    PRODUCT_BUNDLE    = 'bundle'
    OPTION_FIELD      = 'field'
    OPTION_TEXT       = 'text'
    OPTION_SELECT     = 'select'
    OPTION_DATE       = 'date'
    OPTION_DATETIME   = 'datetime'
    OPTION_RADIO      = 'radio'
    OPTION_CHECKBOX   = 'checkbox'
    OPTION_PRICE      = 'price'
    OPTION_BOOLEAN    = 'boolean'
    OPTION_FILE       = 'file'
    OPTION_MULTISELECT = 'multi_select'
    OPTION_FRONTEND   = 'frontend'
    OPTION_BACKEND    = 'backend'
    GENDER_MALE       = 'm'
    GENDER_FEMALE     = 'f'
    GENDER_OTHER      = 'o'
    PRICE_POSITIVE    = '+'
    PRICE_NEGATIVE    = '-'

    def getType(self):
        return self._type

    def _insertParamCharSet(self,data):
        data['charset'] = 'utf8'
        return data


    def _encodeConnectorData(self, data):
        encodeData = {}
        for key,value in data.items():
            encodeData[key] =json.dumps(value)
        return encodeData 

    def curlPost(self,url, data):
        response = requests.post(url,data)
        a=response.json()
        #print(a,'utf8')
        response = a
        return response
        



    def getConnectorData(self,url,data = None):
        if(data):
            data = self._insertParamCharSet(data)
            data = self._encodeConnectorData(data)
        response = self.curlPost(url,data)
        #if response:
            #sreturn False
        return response


    def errorConnector(self,console = True):
        msg = 'Could not connect to Connector!'
        print("Could not connect to Connector!")  
        return {'result':'error','msg':msg}

    def getConnectorUrl(self,action, token = None, type = None):
        if(type == False):
            type = self.getType()
        if(token == False):
            token = self._notice[type]['config']['token']
        url = self.getUrlSuffix(self.CONNECTOR_SUFFIX)
        url = url +  '?action=' + action + '&token=' + token
        #var_dump(url)exit()
        return url

    def getUrlSuffix(self,suffix):
        url = self._cart_url.rstrip('/') + '/' + suffix.lstrip('/')
        return url

    def syncConnectorObject(self,data, extra):
        if (data['data'] and extra['data']):
            for key,rows in extra['data'].items():
                if (key not in data['data']):
                    data['data'][key] = rows
        return data

    def getListFromListByField(self,list, field, value):
        if(list == False):
            return []
        result = []
        for row in list:
            if(row[field] == value):
                result.append(row)
        return result
    
    def duplicateFieldValueFromList(self,lists, field):
        result = []
        if(list == False):
            return result
        for item in lists:
            if (field in item):
                result.append(item[field])
        result = list(set(result))
        return result


    def getRowFromListByField(self, list, field, value):
        if(list == False):
            return False
        result = False
        for row in list:
            if(field in row and row[field] == value):
                result = row
                break 
        return result

    def constructManufacturerLang(self):
        return {'name' : ''}

    def _defaultResponse(self):
        return {'result' : '',
            'msg' : '',
            'elm' : '',
            'data' : {}}




    def processImageBeforeImport(url, path = None):
        file = File()
        if(path == False):
            full_url = url
            path = file.stripDomainFromUrl(url)
        else:
            full_url = file.joinUrlPath(url, path)

        if(file.isUrlEncode(path) == False):
            full_url = file.getRawUrl(full_url)
        if(file.isVirtualUrl(full_url)):
            path = file.getFileNameFromVirtualUrl(full_url, path)
        path = file.changeSpecialCharInPath(path)
        #var_dump(full_url)var_dump(path)exit()
        return {'url' : full_url,
            'path' : path}


    def addPrefixPath(self,path, prefix = ''):
        join_path = ''
        if(prefix):
            join_path = join_path +  prefix.rstrip('\\/') + '/'
        join_path = join_path + path.lstrip('\\/')
        return join_path

    def removePrefixPath(self,path, prefix = ''):
        if(prefix):
            prefix_length = len(prefix)
            path = path[(prefix_length-1):]
        return path


    def datetimeNow(self,format = '%Y-%m-%d %H:%M:%S'):
        return datetime.datetime.now().strftime(format)


    def getMapFieldBySource(self,type, id_src = None, code_src = None, field = 'id_desc'):
        url_src = self._notice['src']['cart_url']
        url_desc = self._notice['target']['cart_url']
        if(code_src):
            id_src = None
        else:
            code_src = None
        #map = self.selectMap(url_src, url_desc, type, id_src, None, code_src)
        conn = self.connectdb()
        query = """select * from cartmigration_map where url_src = '%s' and url_desc = '%s' and type = '%s' and id_src = %d and id_desc = '%s' and code_src = '%s'""" %(url_src,url_desc,type,id_src,None,code_src)
        map = self.selectdb(conn,query)
        if(map == False):
            return False
        if map:
            return map[field]
        else:
            return False


    def getRowFromListByField(list, field, value):
        if(list == False):
            return False
        
        result = False
        for row in list.items():
            try:
                if(row[field] == value):
                    result = row
                    break
            except:
                break 
            
        
        return result
    
    def getNameFromString(self,name):
        result = {}
        parts = name.split(' ')
        result['lastname'] = parts.pop()
        result['firstname'] = ' ' + join(parts)
        return result
    def createNameFromParts(self,first, last, middle = None):
        name = first
        if(middle):
            name =name +  ' ' + middle
        
        name = name + last
        return name
    



    def deleteTargetOrder(self,order_id):
        if(order_id==False):
            return True
        
            
        delete = self.getConnectorData(self.getConnectorUrl('query'), {'serialize' : True,
            'query' : {'order_voucher' : {'type' : 'query',
                    'query' : "DELETE FROM order_voucher WHERE order_id = " + order_id}},
                'order_total' : {'type' : 'query','query' : "DELETE FROM order_total WHERE order_id = " + order_id},
                'order_recurring' : {'type' : 'query',
                    'query' : "DELETE FROM order_recurring WHERE order_id = " + order_id},
                'order_product' : {'type' : 'query',
                    'query' : "DELETE FROM order_product WHERE order_id = " + order_id},
                'order_option' : {'type' : 'query',
                    'query' : "DELETE FROM order_option WHERE order_id = " + order_id},
                'order_history' : {'type' : 'query',
                    'query' : "DELETE FROM order_history WHERE order_id = " + order_id},
                'order_custom_field' : {'type' : 'query',
                    'query' : "DELETE FROM order_custom_field WHERE order_id = " + order_id},
                'order' : {'type' : 'query',
                    'query' : "DELETE FROM `order` WHERE order_id = " + order_id}})
            
        return True
    





    










    # functions to migration taxes table
    def getTaxesMainExport(self):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def getTaxesExtExport(self,taxes):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
    def convertTaxExport(self,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def getTaxIdImport(self,convert,tax,taxesExt):
        return False
    
    def checkTaxImport(self,convert,tax,taxesExt):
        return False


    def routerTaxImport(self,convert,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'}

    def beforeTaxImport(self,convert,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def taxImport(self,convert,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 0}

    def afterTaxImport(self,tax_id,convert,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}   



    def additionTaxImport(self,tax_id,convert,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}} 



    # functions to migration manufacture table
    def getManufacturersMainExport(self):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def getManufacturersExtExport(self,manufacturers):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
    def convertManufacturerExport(self,manufacturer,manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def getManufacturerIdImport(self,convert,manufacturer,manufacturersExt):
        return False
    
    def checkManufacturerImport(self,convert,manufacturer,manufacturersExt):
        return False


    def routerTaxImport(self,convert,tax,taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'}

    def beforeManufacturerImport(self,convert,manufacturer,manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def ManufacturerImport(self,convert,manufacturer,manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 0}

    def afterManufacturerImport(self,manufacturer_id,convert,manufacturer,manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}   



    def additionManufacturerImport(self,manufacturer_id,convert,manufacturer,manufacturersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}} 


    # functions to migration category table
    def getCategoriesMainExport(self):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def getCategoriesExtExport(self,categories):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
    def convertCategoryExport(self,category,categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def getCategoryIdImport(self,convert,category,categoriesExt):
        return False
    
    def checkCategoryImport(self,convert,category,categoriesExt):
        return False


    def routerCategoryImport(self,convert,category,categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'}

    def beforeCategoryImport(self,convert,category,categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def categoryImport(self,convert,category,categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 0}

    def afterCategoryImport(self,category_id, convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}   



    def additionCategoryImport(self,category_id, convert, category, categoriesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}} 

    # functions to migration product table
    def getProductsMainExport(self):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def getProductsExtExport(self,products):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
    def convertProductExport(self,product,productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def getProductIdImport(self,convert,product,productsExt):
        return False
    
    def checkProductImport(self,convert,product,productsExt):
        return False


    def routerProductImport(self,convert,product,productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'}

    def beforeProductImport(self,convert,product,productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def productImport(self,convert,product,productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 0}

    def afterProductImport(self,product_id, convert,product,productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}   



    def additionProductImport(self,product_id, convert,product,productsExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}} 
 

    # functions to migration customer table

    def getCustomersMainExport(self):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def getCustomersExtExport(self,customers):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
    def convertCustomerExport(self,customer,customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def getCustomerIdImport(self,convert,customer,customersExt):
        return False
    
    def checkCustomerImport(self,convert,customer,customersExt):
        return False


    def routerCustomerImport(self,convert,customer,customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'}

    def beforeCustomerImport(self,convert,customer,customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def customerImport(self,convert,customer,customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 0}

    def afterCustomerImport(self,customer_id, convert,customer,customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}   



    def additionCustomerImport(self,customer_id, convert,customer,customersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    # functions to migration order table

    def getOrdersMainExport(self):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def getOrdersExtExport(self,customers):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    
    def convertOrderExport(self,order,ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def getOrderIdImport(self,convert,order,ordersExt):
        return False
    
    def checkOrderImport(self,convert,order,ordersExt):
        return False


    def routerOrderImport(self,convert,order,ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'}

    def beforeOrderImport(self,convert,order,ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def orderImport(self,convert,order,ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 0}

    def afterOrderImport(self,order_id, convert,order,ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}   



    def additionOrderImport(self,order_id, convert,order,ordersExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}    

    

    ########maping ructor
    

    def constructTax(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'created_at' : None,
            'updated_at' : None,
            'tax_products' : {},
            'tax_customers' : {},
            'tax_zones' : {},
            'languages' : {}}


    def constructTaxProduct(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'created_at' : None,
            'updated_at' : None,
            'languages' : {}}



    def constructTaxCustomer(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'created_at' : None,
            'updated_at' : None,
            'languages' : {}}


    def constructTaxZone(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'created_at' : None,
            'updated_at' : None,
            'country' : {},
            'state' : {},
            'rate' : {},
            'languages' : {}}



    def constructTaxZoneCountry(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'country_code' : '',
            'created_at' : None,
            'updated_at' : None,
            'languages' : {}}




    def constructTaxZoneState(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'state_code' : '',
            'created_at' : None,
            'updated_at' : None,
            'languages' : {}}


    def constructTaxZoneRate(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'rate' : '',
            'priority' : 0,
            'created_at' : None,
            'updated_at' : None,
            'languages' : {}}




    def constructManufacturer():
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'name' : '',
            'url' : '',
            'image' : {'label' : '',
                'url' : '',
                'path' : ''},
            'created_at' : None,
            'updated_at' : None,
            'languages' : {}}




    def constructManufacturerLang(self):
        return {'name' : ''}



    def constructCategory(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'parent' : {},
            'active' : False,
            'image' : {'label' : '',
                'url' : '',
                'path' : ''},
            'name' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : '',
            'sort_order' : 0,
            'created_at' : None,
            'updated_at' : None,
            'languages' : {},
            'category' : {},
            'categoriesExt' : {}}


    def constructCategoryParent(self):
        return {'id' :None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'parent' : {},
            'active' : False,
            'image' : {'label' : '',
                'url' : '',
                'path' : '',},
            'name' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : '',
            'sort_order' : 0,
            'created_at' : None,
            'updated_at' : None,
            'languages' : {},
            'category' : {},
            'categoriesExt' : {}}

    def constructCategoryLang(self):
        return {'name' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : ''}


    def constructProduct(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'type' : '',
            'image' : {'label' : '',
                'url' : '',
                'path' : ''},
            'images' : {},
            'name' : '',
            'sku' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : '',
            'tags' : '',
            'price' : '',
            'special_price' : {'price' : '',
                'start_date' : '',
                'end_date' : ''},
            'group_prices' : {},
            'tier_prices' : {},
            'weight' : '',
            'length' : '',
            'width' : '',
            'height' : '',
            'status' : false,
            'manage_stock' : true,
            'qty' : 0,
            'tax' : {'id' : None,
                'code' : None},
            'manufacturer' : {'id' : None,
                'code' : None,
                'name' : None},
            'created_at' : None,
            'updated_at' : None,
            'categories' : {},
            'languages' : {},
            'options' : {},
            'attributes' : {},
            'children' : {},
            'group_products' : {}}

    def constructProductImage(self):
        return {'label' : '',
            'url' : '',
            'path' : ''}

    def constructProductGroupPrice(self):
        return {'id' : None,
            'code' : None,
            'sites' : {},
            'languages' : {},
            'name' : '',
            'group_code' : '',
            'group' : {},
            'price' : '',
            'start_date' : '',
            'end_date' : ''}

    def constructProductTierPrice(self):
        return {'id' : None,
            'code' : None,
            'sites' : {},
            'languages' : {},
            'name' : '',
            'tier_code' : '',
            'group' : {},
            'qty' : 0,
            'price' : '',
            'start_date' : '',
            'end_date' : ''}

    def constructProductCategory(self):
        return {'id' : None,
            'code' : None}

    def constructProductLang(self):
        return {'name' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : ''}

    def constructProductAttribute(self):
        return {'option_id' : None,
            'option_code_save' : None,
            'option_set' : '',
            'option_group' : '',
            'option_mode' : 'backend',
            'option_type' : '',
            'option_code' : '',
            'option_name' : '',
            'option_languages' : {},
            'option_value_id' : None,
            'option_value_code_save' : None,
            'option_value_code' : '',
            'option_value_name' : '',
            'option_value_languages' : {},
            'price' : '0.0000',
            'price_prefix' : '+'}

    def constructProductOption(self):
        return {'id' : None,
            'code' : None,
            'option_set' : '',
            'option_group' : '',
            'option_mode' : 'backend',
            'option_type' : '',
            'option_code' : None,
            'option_name' : '',
            'option_languages' : {},
            'required' : False,
            'values' : {}}

    def constructProductOptionLang(self):
        return {'option_name' : ''}

    def constructProductOptionValue(self):
        return {'id' : None,
            'code' : None,
            'option_value_code' : None,
            'option_value_name' : '',
            'option_value_languages' : {},
            'price' : '0.0000',
            'price_prefix' : '+'}

    def constructProductOptionValueLang(self):
        return {'option_value_name' : ''}

    def constructChildProduct(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'type' : '',
            'image' : {'label' : '',
                'path' : '',
                'url' : ''},
            'images' : {},
            'name' : '',
            'sku' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : '',
            'price' : '',
            'special_price' : {'price' : '',
                'start_date' : '',
                'end_date' : ''},
            'weight' : '',
            'length' : '',
            'width' : '',
            'height' : '',
            'status' : False,
            'manage_stock' : False,
            'qty' : 0,
            'tax' : {'id' : None,
                'code' : None},
            'manufacturer' : {'id' : None,
                'code' : None},
            'created_at' : None,
            'updated_at' : None,
            'categories' : {},
            'languages' : {},
            'attributes' : {}}

    def constructChildProductLang(self):
        return {'name' : '',
            'description' : '',
            'short_description' : '',
            'meta_title' : '',
            'meta_keyword' : '',
            'meta_description' : ''}

    def constructChildProductAttribute(self):
        return {'option_id' : None,
            'option_code_save' : None,
            'option_set' : '',
            'option_group' : '',
            'option_mode' : '',
            'option_type' : '',
            'option_code' : None,
            'option_name' : '',
            'option_languages' : {},
            'option_value_id' : None,
            'option_value_code_save' : None,
            'option_value_code' : None,
            'option_value_name' : '',
            'option_value_languages' : {},
            'price' : '0.0000',
            'price_prefix' : '+'}

    def constructProductGroupProduct(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'type' : '',
            'name' : '',
            'sku' : '',
            'price' : '',
            'qty' : '',
            'sort_order' : ''}

    def constructCustomer(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
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

    def constructCustomerAddress(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'first_name' : '',
            'middle_name' : '',
            'last_name' : '',
            'gender' : '',
            'address_1' : '',
            'address_2' : '',
            'city' : '',
            'country' : {'id' : None,
                'code' : None,
                'country_code' : '',
                'name' : ''},
            'state' : {'id' : None,
                'code' : None,
                'state_code' : '',
                'name': ''},
            'postcode' : '',
            'telephone' : '',
            'company' : '',
            'fax' : '',
            'default' : {'billing' : False,
                'shipping' : False},
            'billing' : False,
            'shipping' : False,
            'created_at' : None,
            'updated_at' : None}

    def constructCustomerGroup(self):
        return {'id' : None,
            'code' : None}

    def constructOrder(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'status' : '',
            'tax' : {'title' : '',
                'amount' : '',
                'percent' : ''},
            'discount' : {'code' : '',
                'title' : '',
                'amount' : '',
                'percent' : ''},
            'shipping' : {'title' : '',
                'amount' : '',
                'percent' : ''},
            'subtotal' : {'title' : '',
                'amount' : ''},
            'total' : {'title' : '',
                'amount' : ''},
            'currency' : '',
            'created_at' : None,
            'updated_at' : None,
            'customer' : {},
            'customer_address' : {},
            'billing_address' : {},
            'shipping_address' : {},
            'payment' : {},
            'items' : {},
            'histories' : {}}

    def constructOrderCustomer(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'username' : '',
            'email' : '',
            'first_name' : '',
            'middle_name' : '',
            'last_name' : ''}

    def constructOrderAddress(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'first_name' : '',
            'middle_name' : '',
            'last_name' : '',
            'address_1' : '',
            'address_2' : '',
            'city' : '',
            'country' : {'id' : None,
                'code' : None,
                'country_code' : '',
                'name' : ''},
            'state' : {'id' : None,
                'code' : None,
                'state_code' : '',
                'name': ''},
            'postcode' : '',
            'telephone' : '',
            'company' : '',
            'fax' : ''}

    def constructOrderPayment(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'method' : '',
            'title' : ''}

    def constructOrderItem(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'product' : {'id' : None,
                'code' : None,
                'name' : '',
                'sku' : ''},
            'qty' : 0,
            'price' : '',
            'original_price' : '',
            'tax_amount' : '',
            'tax_percent' : '',
            'discount_amount' : '',
            'discount_percent' : '',
            'subtotal' : '',
            'total' : '',
            'options' : {},
            'created_at' : None,
            'updated_at' : None}

    def constructOrderItemOption(self):
        return {'option_id' : '',
            'option_code_save' : '',
            'option_set' : '',
            'option_group' : '',
            'option_type' : '',
            'option_code' : '',
            'option_name' : '',
            'option_value_id' : '',
            'option_value_code_save' : '',
            'option_value_code' : '',
            'option_value_name' : '',
            'price' : '0.0000',
            'price_prefix' : '+'}

    def constructOrderHistory(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'status' : '',
            'comment' : '',
            'notified' : false,
            'created_at' : None,
            'updated_at' : None}

    def constructReview(self):
        return {'id' : None,
            'code' : None,
            'site_id' : '',
            'language_id' : '',
            'product' : {'id' : None,
                'code' : None,
                'name' : ''},
            'customer' : {'id' : None,
                'code' : None,
                'name' : ''},
            'title' : '',
            'content' : '',
            'status' : '',
            'created_at' : None,
            'updated_at' : None,
            'rating' : {}}

    def constructReviewRating(self):
        return {'id' : None,
            'code' : None,
            'rate_code' : '',
            'rate' : ''}


    def iset(self,_array,_key):
        if (('csl' in b) and (b['csl'] != None)):
            return True
        else:
            return False

    def arrayToCount(self,array, name = False):
        if( not array):
            return 0
        
        count = 0
        if name:
            count = array[0][name] if self.iset(array[0],name) else 0
        else:
            count = array[0][0] if self.iset(array[0],0) else 0
        
        return count

#cart = Cartmvc()































'''
    def getCustomersMainExport(self,last_id_f):
        query = "SELECT * FROM customers where customers_id >%d and customers_id < %d"  %(last_id_f,last_id_f+12)
        query = {'query':{'type':'select', 'query':query},}
        customers = self.getConnectorData('http://localhost/oscommerce/catalog/cartmigration_connector/connector.php?action=query&token=123456', query)
        #print(customers)
        return customers
        
    

    def ructCustomer(self):
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







    def convertCustomerExport(self,customer,i):
        customer_data = self.ructCustomer()
        customer_data['id'] = customer['data'][i]['customers_id']
        customer_data['email'] = customer['data'][i]['customers_email_address']
        customer_data['password'] = customer['data'][i]['customers_password']
        customer_data['first_name'] = customer['data'][i]['customers_firstname']
        customer_data['last_name'] = customer['data'][i]['customers_lastname']
        customer_data['gender'] = customer['data'][i]['customers_gender']
        customer_data['dob'] = customer['data'][i]['customers_dob']
        customer_data['telephone'] = customer['data'][i]['customers_telephone']
        customer_data['fax'] = customer['data'][i]['customers_fax']
        customer_data['active'] = True

        return  customer_data




    def convertStringToDatetime(self,string):
        date = date_create(string)
        return date_format(date,'Y-m-d H:i:s') 


    def customerImport(self,convert):
      
        fields_table = ["customer_group_id","store_id","language_id","firstname","lastname","email","telephone","fax","password","salt","cart","wishlist","newsletter","address_id","custom_field","ip","status","approved","safe","token","code","date_added"]
        
        data_insert = {
            'firstname' : convert['first_name'],
            'lastname' : convert['last_name'],
            'approved' : 1,
            'fax' : convert['fax'],
            'telephone' : convert['telephone'],
            'email' : convert['email'],
            'password' : convert['password'],
            'newsletter' : 0,
            'status' : 1,
            'safe' : 1,
            'salt' : ''
        }

        customer_query = """INSERT INTO oc_customer(customer_group_id, firstname, lastname, approved, fax, telephone, email, password, newsletter, status, safe, salt) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')""" % ('', data_insert['firstname'], data_insert['lastname'], '', data_insert['fax'], data_insert['telephone'], data_insert['email'], data_insert['password'], data_insert['newsletter'], 1, 1, data_insert['salt'])
        customer_import = self.getConnectorData('http://localhost/opencart/cartmigration_connector/connector.php?action=query&token=123456', {
            'query' : {
                'type' : 'insert',
                'query' : customer_query,
                'params' : {
                    'insert_id' : True,
                }
                }}
        )



def migrate(last_id):
    last_id_f = last_id
    cart = Cart()
    while 1:
        customer = cart.getCustomersMainExport(last_id_f)
        if not customer['data']:
            break
        for x in range(0, 11):
            convert = cart.convertCustomerExport(customer,x)
            if convert:    
                cart.customerImport(convert)
                #print(x+1)
                last_id_f = last_id_f +  1
                #print('%s' %last_id_f)
                cnx = mysql.connector.connect(user='root',password = '', database='carttest')
                cursor = cnx.cursor()
                sql = """update resume set id_src = %d""" %(last_id_f)
                try:
                    cursor.execute(sql)
                    cnx.commit()
                except:
                    cnx.rollback()
            else:
                break
    print(last_id_f)

class myThread (threading.Thread):
    def __init__(self, threadID, name, counter,last_id):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.last_id = last_id
    def run(self):
        print ("Starting " + self.name)
        migrate(self.last_id)
        print ("Exiting " + self.name)



conn = mysql.connector.connect(user = 'root',password = '',host = 'localhost', database = 'carttest')
mycursor=conn.cursor()
query = ("SELECT id_src from resume")
mycursor.execute(query)
result = mycursor.fetchall()
last_id = result[0][0]

thread1 = myThread(1, "Thread-1", 1,last_id)
thread2 = myThread(2, "Thread-2", 2,last_id)
thread3 = myThread(3, "Thread-1", 3,last_id)
thread4 = myThread(4, "Thread-2", 4,last_id)
thread5 = myThread(5, "Thread-1", 5,last_id)
thread6 = myThread(6, "Thread-1", 6,last_id)
thread7 = myThread(7, "Thread-2", 7,last_id)
thread8 = myThread(8, "Thread-1", 8,last_id)
thread9 = myThread(9, "Thread-2", 9,last_id)
thread10 = myThread(10, "Thread-1", 10,last_id)
# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()
thread8.start()
thread9.start()
thread10.start()
print(threading.activeCount())
thread1.join()
thread2.join()
thread3.join()
thread4.join()
thread5.join()
thread6.join()
thread7.join()
thread8.join()
thread9.join()
thread10.join()
print ("Exiting Main Thread")



#cnx = mysql.connector.connect(user='root',password = '', database='carttest')
#cnx.close()
print('end')
'''