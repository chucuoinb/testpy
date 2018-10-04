import base64
import requests
import pickle
import requests
import urllib.parse
import json
import mysql.connector
import threading
import time

from cartmvc import Cartmvc
from config import ConfigInfo
from urllib.parse import urlparse

class Shoppify(Cartmvc,ConfigInfo):
	DEBUG_MODE = True
    _api_url = None



     def ConfigSource(self):

        response = self._defaultResponse()
        #Main config
        api_shop = self.api('/admin/shop.json')
        if (api_shop == False):
            return {'result' : 'warning',
                'elm' : '#error-api',
        'msg' : 'Shopify API info is not correct!'}
            
        
        shop = json.load(api_shop)
        currency = shop['shop']['currency'] if self.iset(shop['shop'],'currency') else "USD"
        self._notice['src']['language_default'] = 1
        self._notice['src']['currency_default'] = currency
        self._notice['src']['category_root'] = 1
        self._notice['src']['site'] = {1 : 'Default Shop'}
        
        self._notice['src']['categoryData'] = {1 : 'Default Category'}
        
        self._notice['src']['attributes'] = {1 : 'Default Attribute'}
        
        language_data = {1 : "Default Language"}
        
        order_status_data = {'pending' : "Pending",
            'authorized' : "Authorized",
            'partially_paid' : "Partially Paid",
            'paid' : "Paid",
            'partially_refunded' : "Partially Refunded",
            'refunded' : "Refunded",
            'voided' : "Voided"}
        
        currency_data = {currency : currency}
        self._notice['src']['languages'] = language_data
        self._notice['src']['order_status'] = order_status_data
        self._notice['src']['currencies'] = currency_data
        self._notice['src']['support']['country_map'] = False
        self._notice['src']['support']['customer_group_map'] = False
        self._notice['src']['support']['manufacturers'] = False
        self._notice['src']['support']['reviews'] = False

    self._notice['src']['support']['add_new'] = True
        self._notice['src']['support']['clear_shop'] = True
    self._notice['src']['support']['pre_cus'] = False
        self._notice['src']['support']['pre_ord'] = False
        response['result'] = 'success'
        return response
    

    def ConfigTarget(self):
        
        response = self._defaultResponse()
        #Main config
        api_shop = self.api('/admin/shop.json')
        if (api_shop == False):
            return {'result' : 'warning',
                'elm' : '#error-api',
                'msg' : 'Shopify API info is not correct!'}
            
        
        shop = json.load(api_shop)
        currency = shop['shop']['currency'] if self.iset(shop['shop'],'currency') else "USD"
        self._notice['target']['language_default'] = 1
        self._notice['target']['currency_default'] = currency
        self._notice['target']['category_root'] = 1
        self._notice['target']['support']['site_map'] = False
        self._notice['target']['support']['category_map'] = False
        self._notice['target']['support']['attribute_map'] = False
        #Extra Config
        country_data = {}
        language_data = {1 : "Default Language"}
        
        order_status_data = {'pending' : "Pending",
            'authorized' : "Authorized",
            'partially_paid' : "Partially Paid",
            'paid' : "Paid",
            'partially_refunded' : "Partially Refunded",
            'refunded' : "Refunded",
            'voided' : "Voided"}
        
        currency_data = {currency : currency}
        if (self._notice['src']['support']['pages']):
            self._notice['target']['support']['pages'] = True
        
        self._notice['target']['support']['language_map'] = False
        self._notice['target']['languages'] = language_data
        self._notice['target']['support']['order_status_map'] = True
        self._notice['target']['order_status'] = order_status_data
        self._notice['target']['support']['currency_map'] = True
        self._notice['target']['currencies'] = currency_data
        self._notice['target']['support']['country_map'] = False
        self._notice['target']['countries'] = country_data
        self._notice['target']['support']['customer_group_map'] = False
        self._notice['target']['support']['taxes'] = True
        self._notice['target']['support']['manufacturers'] = False
        self._notice['target']['support']['categories'] = True
        self._notice['target']['support']['products'] = True
        self._notice['target']['support']['customers'] = True
        self._notice['target']['support']['orders'] = True
        self._notice['target']['support']['reviews'] = True

        self._notice['target']['support']['add_new'] = True
        self._notice['target']['support']['clear_shop'] = True
    self._notice['target']['support']['pre_cus'] = False
        self._notice['target']['support']['pre_ord'] = True
        response['result'] = 'success'
        return response
    






	def getTaxesMainExport(self):
        taxes = {'id' : '1',
            'code' : 'Tax Rule Shopify'}
        return {'result' : 'success',
            'msg' : '',
            'data' : [taxes]}


    def api(self,path, data = None, type = 'Get', header = ["Content-Type: application/json"]):
        time.sleep(5)
        api_url = self.getApiUrl()
        url = api_url + path
        functions = 'curl' + type
        if (self.DEBUG_MODE) :
            functions = '_curl' + type
        if (isinstance(data, (frozenset, list, set, tuple))):
            data = json.load(data)
        return self.functions(url, data, header)

    
    def getTaxesExtExport(taxes):
        taxRates = self.api('/admin/countries.json')
        if (taxRates == False):
            return {'result' : "error",
                'msg' : "Could not get data from Shopify"}
        tax_rates = json.load(taxRates, 1)
        return {'result' : 'success',
            'msg' : '',
            'data' : tax_rates}



    def convertTaxExport(self,tax, taxesExt):
        taxProduct = taxCustomer = taxZone = []

        tax_product = self.constructTaxProduct()
        tax_product['id'] = 1
        tax_product['code'] = None
        tax_product['name'] = 'Product Tax Class Shopify'
        taxProduct[] = tax_product

        for country in taxesExt['data']['countries'].items():
            if ((self.iset(country,'provinces'))):
                for province in country['provinces'].items():
                    tax_zone_state = self.constructTaxZoneState()
                    tax_zone_state['id'] = province['id']
                    tax_zone_state['name'] = province['name']
                    tax_zone_state['state_code'] = province['code']

                    tax_zone_country = self.constructTaxZoneCountry()
                    tax_zone_country['id'] = country['id']
                    tax_zone_country['name'] = country['name']
                    tax_zone_country['country_code'] = country['code']

                    tax_zone_rate = self.constructTaxZoneRate()
                    tax_zone_rate['id'] = None
                    tax_zone_rate['name'] = province['tax_name']
                    tax_zone_rate['rate'] = province['tax']

                    tax_zone = self.constructTaxZone()
                    tax_zone['id'] = None
                    tax_zone['name'] = country['tax_name'] + ' - ' + province['tax_name'] if(province['tax_name']) else country['tax_name'] + ' - ' + province['code']
                    tax_zone['country'] = tax_zone_country
                    tax_zone['state'] = tax_zone_state
                    tax_zone['rate'] = tax_zone_rate

                    taxZone.append(tax_zone)
            else:
                tax_zone_state = self.constructTaxZoneState()

                tax_zone_country = self.constructTaxZoneCountry()
                tax_zone_country['id'] = country['id']
                tax_zone_country['name'] = country['name']
                tax_zone_country['country_code'] = country['code']

                tax_zone_rate = self.constructTaxZoneRate()
                tax_zone_rate['id'] = None
                tax_zone_rate['name'] = country['tax_name']
                tax_zone_rate['rate'] = country['tax']

                tax_zone = self.constructTaxZone()
                tax_zone['id'] = None
                tax_zone['name'] = country['tax_name'] if country['tax_name'] else country['code']
                tax_zone['country'] = tax_zone_country
                tax_zone['state'] = tax_zone_state
                tax_zone['rate'] = tax_zone_rate

                taxZone.append(tax_zone)

        tax_data = self.constructTax()
        tax_data['id'] = tax['id']
        tax_data['name'] = tax['code']
        tax_data['tax_products'] = taxProduct
        tax_data['tax_zones'] = taxZone

        return {'result' : 'success',
            'msg' : '',
            'data' :tax_data}


    def getTaxIdImport(convert, tax, taxesExt):
        return convert['id']

    def checkTaxImport(convert, tax, taxesExt):
        return self.getMapFieldBySource(self.TYPE_TAX, convert['id'], convert['code'])

    def routerTaxImport(convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'} #taxImport - beforeTaxImport - additionTaxImport


    def beforeTaxImport(convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def taxImport(convert, tax, taxesExt):
        url_src = self._notice['src']['cart_url']
        url_target = self._notice['target']['cart_url']

        # tax class
        # geo zone
        # geo zone - zone - country
        # tax rate
        allCountries = self.api('/admin/countries.json')
        if (allCountries == False):
            return {'result' : "error",
                'msg' : "Could not get countries data from Shopify"}
        allCountries = json.load(allCountries)

        if ((self.iset(convert,'tax_zones')) == False or (not convert['tax_zones'])):
            return {'result' : "warning",
                'msg' : "Tax id " + convert['id'] + " import failed. Error: Tax zones are not existed!"}

        taxZones = convert['tax_zones']
        for tax_zone in taxZones.items():
            country_code = tax_zone['country']['country_code']
            state_code = tax_zone['state']['state_code']
            rate = tax_zone['rate']['rate'] / 100
            check_country = False
            id_country = 0
            for country in  allCountries['countries'].items():
                if (country['code'] == country_code):
                    check_country = True
                    id_country = country['id']

            if (check_country == False):
                postData = {'country' : {'code' : country_code}}
                    #'tax' : rate
                response = self.api('/admin/countries.json', postData, 'Post')
                response = json.load(response)
                checkResponse = self.checkResponseImport(response, {'id' : country_code, 'code' : country_code}, 'country')
                if (checkResponse['result'] != 'success'):
                    return checkResponse
                id_country = response['country']['id']
            if (state_code == False and id_country):
                putData = {'country' : {'id' : id_country,
                        'tax' : rate}}
                response = self.api('/admin/countries/' + id_country + '.json', putData, 'Put')
                response = json.load(response)
                checkResponse = self.checkResponseUpdate(response, country_code, 'country')
                if (checkResponse['result'] != 'success'):
                    return checkResponse
                continue


            countryDetail = self.api('/admin/countries/' + id_country + '.json')
            if (countryDetail == False):
                return {'result' : 'warning',
                    'msg' : 'Could not get data country: ' + country_code}

            countryDetail = json.load(countryDetail)
            check_state = False
            id_state = 0
            for  province in  countryDetail['country']['provinces'].items():
                if (province['code'] == state_code):
                    check_state = True
                    id_state = province['id']

            if (check_state):
                putData = {'province' : {'id' : id_state,
                        'tax' : rate}}
                response = self.api('/admin/countries/' + id_country + '/provinces/' + id_state + '.json', putData, 'Put')
                response = json.load(response)
                checkResponse = self.checkResponseUpdate(response, country_code + ':' + state_code, 'province')
                if (checkResponse['result'] != 'success'):
                    return checkResponse

        return {'result' : 'success',
            'msg' : '',
            'data' : 0}


    def afterTaxImport(self,tax_id, convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

    def additionTaxImport(tax_id, convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}





















    def checkResponseImport(response, convert, type = ''):

        if (self.DEBUG_MODE and ("response['errors']" in vars())):
            console = []
            id = convert['id'] if convert['id'] else convert['code']
            if (isinstance(response['errors'], (list,tuple))):
                response['errors'] = [(response['errors'])]
            
            for key, error in response['errors'].items():

                if (isinstance(error, (list,tuple))):
                    error_messages = error.join(' ')
                else:
                    error_messages = error
                
                console.append(key + ': ' + error_messages)
            
            console_warning = console.join('.')
            return {'result' : 'warning',
                'msg' : ' id ' + id + ' import failed. Error: '}
        else if (response == False):
            return {'result' : 'warning',
                'msg' : ' id ' + id + ' import failed!'}
        else:
            return {'result' : 'success'}


    def getApiUrl(self):
        if (self._api_url == False):
            self._api_url = self._createApiUrl()
        return self._api_url

    def _createApiUrl(self):
        url = urlparse(self._cart_url)
        if (self._type != 'src'):
            api_key = trimself._notice['target']['config']['api']['api_key'].strip()
            password = trimself._notice['target']['config']['api']['password'].strip()
        else:
            api_key = self._notice['src']['config']['api']['api_key'].strip()
            password = self._notice['src']['config']['api']['password'].strip()
        api_url = 'https:#' + api_key + ':' + password + '@' + url['host']
        if (self.iset(url,'path')):
            api_url = api_url + url['path']
        return api_url
    


    def _curlPost(self,url, data = None):


        response = requests.post(url,data)
        response=response.json()
        #print(a,'utf8')
        return response    

    def _curlGet(self,url, data = None):
        response = requests.get(url,data)
        response=response.json()
        #print(a,'utf8')
        return response 

    def _curlPut(self,url, data = None):
        response = requests.Put(url,data)
        response=response.json()
        #print(a,'utf8')
        return response 

    def _curlDelete(self,url, data = None):
        response = requests.delete(url,data)
        response=response.json()
        #print(a,'utf8')
        return response         
    























