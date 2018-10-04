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

class Volusion(Cartmvc,ConfigInfo):
	IMG_GALERY = False
    VLS_CUR = 'cartmigration_volusion_currency'
    VLS_TAX = 'cartmigration_volusion_tax'
    VLS_CAT = 'cartmigration_volusion_category'
    VLS_PRO = 'cartmigration_volusion_product'
    VLS_OPT_CAT = 'cartmigration_volusion_option_category'
    VLS_OPT = 'cartmigration_volusion_option'
    VLS_KIT = 'cartmigration_volusion_kit'
    VLS_KIT_LNK = 'cartmigration_volusion_kit_lnk'
    VLS_PRO_IMAGE = 'cartmigration_volusion_product_image'
    VLS_CUS = 'cartmigration_volusion_customer'
    VLS_ORD = 'cartmigration_volusion_order'
    VLS_ORD_DTL = 'cartmigration_volusion_order_detail'
    VLS_REV = 'cartmigration_volusion_review'


    def getFileInfo(self):
        if (self.IMG_GALERY):
            return {'exchangeRates' : 'ExchangeRates',
                'taxes' : 'Tax',
                'categories' : 'Categories',
                'products' : 'Products',
                'optionCategories' : 'OptionCategories',
                'options' : 'Options',
                'kits' : 'KITS',
                'kitLinks' : 'KITLNKS',
                'images' : 'Product Images',
                'customers' : 'Customers',
                'orders' : 'Orders',
                'orderDetails' : 'OrderDetails',
                'reviews' : 'Reviews'}
        else:
            return {'exchangeRates' : 'ExchangeRates',
                'taxes' : 'Tax',
                'categories' : 'Categories',
                'products' : 'Products',
                'optionCategories' : 'OptionCategories',
                'options' : 'Options',
                'kits' : 'KITS',
                'kitLinks' : 'KITLNKS',
                'customers' : 'Customers',
                'orders' : 'Orders',
                'orderDetails' : 'OrderDetails',
                'reviews' : 'Reviews'}


    def taxesTableConstruct(self):
        return {'table' : self.VLS_TAX,
            'rows' : {'folder' : 'VARCHAR(255)',
                'url_src' : 'VARCHAR(255)',
                'url_desc' : 'VARCHAR(255)',
                'taxid' : 'BIGINT',
                'taxstateshort' : 'TEXT',
                'taxstatelong' : 'TEXT',
                'taxcountry' : 'TEXT',
                'tax1_title' : 'TEXT',
                'tax2_title' : 'TEXT',
                'tax3_title' : 'TEXT',
                'tax1_percent' : 'TEXT',
                'tax2_percent' : 'TEXT',
                'tax3_percent' : 'TEXT'}}

    def optionsTableConstruct(self):
        return {'table' : self.VLS_OPT,
            'rows' : {'folder' : 'VARCHAR(255)',
                'url_src' : 'VARCHAR(255)',
                'url_desc' : 'VARCHAR(255)',
                'id' : 'BIGINT',
                'optioncatid' : 'BIGINT',
                'optionsdesc' : 'TEXT',
                'pricediff' : 'TEXT'}
            'validation' : {'id', 'optioncatid'}}



    def setupStorageCsv(self):
        tables = []
        queries = []
        creates = {'exchangeRatesTableConstruct',
            'taxesTableConstruct',
            'categoriesTableConstruct',
            'productsTableConstruct',
            'optionCategoriesTableConstruct',
            'optionsTableConstruct',
            'kitsTableConstruct',
            'kitLinksTableConstruct',
            'imagesTableConstruct',
            'customersTableConstruct',
            'ordersTableConstruct',
            'orderDetailsTableConstruct',
            'reviewsTableConstruct'}
        for create in creates:
            tables.append(eval('self' + create + '()'))
        for table in tables.items():
            table_query = self.arrayToCreateTableSql(table)
            if (table_query['result'] != 'success'):
                return self.errorDatabase(False)
            queries.append(table_query['query'])
        for query in queries.items():
            self.queryRaw(query)

       self._notice['src']['storage']['result'] = 'process'
        self._notice['src']['storage']['function'] = 'clearStorageCsv'
        self._notice['src']['storage']['msg'] = ""
        return self._notice['src']['storage']


     def prepareImportSource(self):
        return {'result' : 'success'}

    def prepareImportTarget(self):
        return {'result' : 'success'}

    def prepareTaxesImport(self):
        return self

    def prepareTaxesExport(self):
        return self


    def getTaxesMainExport(self):
        #db = self.getDb()
        folder = self._notice['src']['config']['folder']
        source_url = self._notice['src']['cart_url']
        target_url = self._notice['target']['cart_url']
        id_src = self.get_id_last(Taxe)
        limit = self._notice['setting']['taxes']
        table = VLS_TAX
        #table = db->getTableName(self.VLS_TAX)
        query = "SELECT * FROM " + table + " WHERE folder = '" + folder + "' AND url_src = '" + source_url + "' AND url_desc = '" + target_url + "' AND taxid > " + id_src + " ORDER BY taxid ASC LIMIT " + limit
        conn = self.connectdb()
        result = self.selectdb(query)
        if (result == False):
            return 'error database'
        return result

    def getTaxesExtExport(self,taxes):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}


    def convertTaxExport(self,tax, taxesExt):
        taxZone = {}
        tax_zone = self.constructTaxZone()
        if (tax['tax1_title'] and tax['tax1_percent']):
            tax_zone['id'] = tax['taxid']
            tax_zone['name'] = tax['tax1_title']
            tax_zone['country'] = {'name' : tax['taxcountry'],
                'code' : self.getCountryCode(tax['taxcountry']),
                'country_code' : self.getCountryCode(tax['taxcountry'])}
            tax_zone['state'] = {'name' : tax['taxstatelong'],
                'code' : tax['taxstateshort'],
                'state_code' : tax['taxstateshort']}
            tax_zone['rate']['id'] = tax['taxid']
            tax_zone['rate']['code'] = tax['taxstateshort']
            tax_zone['rate']['rate'] = tax['tax1_percent']
            taxZone[] = tax_zone
        if (tax['tax2_title'] and tax['tax2_percent']):
            tax_zone['id'] = tax['taxid']
            tax_zone['name'] = tax['tax2_title']
            tax_zone['country'] = {'name' : tax['taxcountry'],
                'code' : self.getCountryCode(tax['taxcountry']),
                'country_code' : self.getCountryCode(tax['taxcountry'])}
            tax_zone['state'] = {'name' : tax['taxstatelong'],
                'code' : tax['taxstateshort'],
                'state_code' : tax['taxstateshort']}
            tax_zone['rate']['id'] = tax['taxid']
            tax_zone['rate']['code'] = tax['taxstateshort']
            tax_zone['rate']['rate'] = tax['tax2_percent']
            taxZone[] = tax_zone
        if (tax['tax3_title'] and tax['tax3_percent']):
            tax_zone['id'] = tax['taxid']
            tax_zone['name'] = tax['tax3_title']
            tax_zone['country'] = {'name' : tax['taxcountry'],
                'code' : self.getCountryCode(tax['taxcountry']),
                'country_code' : self.getCountryCode(tax['taxcountry'])}
            tax_zone['state'] = {'name' : tax['taxstatelong'],
                'code' : tax['taxstateshort'],
                'state_code' : tax['taxstateshort']}
            tax_zone['rate']['id'] = tax['taxid']
            tax_zone['rate']['code'] = tax['taxstateshort']
            tax_zone['rate']['rate'] = tax['tax3_percent']
            taxZone[] = tax_zone

        tax_data = self.constructTax()
        tax_data = self.addConstructDefault(tax_data)
        tax_data['id'] = tax['taxid']
        tax_data['name'] = tax['taxstateshort']
        tax_data['tax_zones'] = taxZone

        return {'result' : 'success',
            'msg' : '',
            'data' : tax_data}

    def getTaxIdImport(self,convert, tax, taxesExt):
        return convert['id']

    #def checkTaxImport(self,convert, tax, taxesExt):
        #return self.getMapFieldBySource(self.TYPE_TAX, convert['id'], convert['code']) ? True : False

    def routerTaxImport(self,convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'} #taxImport - beforeTaxImport - additionTaxImport

    def beforeTaxImport(self,convert, tax, taxesExt):
        return {'result' : 'success',
            'msg' : '',
            'data' : {}}

     def routerTaxImport(convert, tax, taxesExt)
        return {'result' : 'success',
            'msg' : '',
            'data' : 'taxImport'} #//taxImport - beforeTaxImport - additionTaxImport


    def taxImport(self,convert, tax, taxesExt):
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








    def addConstructDefault(self,construct):
        construct['site_id'] = 1
        construct['language_id'] = self._notice['src']['language_default']
        return construct

    def getCountryCode(self,country):
        country_def = {'AF' : "Afghanistan",
            'AX' : "Aland Islands",
            'AL' : "Albania",
            'DZ' : "Algeria",
            'AS' : "American Samoa",
            'AD' : "Andorra",
            'AO' : "Angola",
            'AI' : "Anguilla",
            'AQ' : "Antarctica",
            'AG' : "Antigua and Barbuda",
            'AR' : "Argentina",
            'AM' : "Armenia",
            'AW' : "Aruba",
            'AP' : "Asia/Pacific Region",
            'AU' : "Australia",
            'AT' : "Austria",
            'AZ' : "Azerbaijan",
            'BS' : "Bahamas",
            'BH' : "Bahrain",
            'BD' : "Bangladesh",
            'BB' : "Barbados",
            'BY' : "Belarus",
            'BE' : "Belgium",
            'BZ' : "Belize",
            'BJ' : "Benin",
            'BM' : "Bermuda",
            'BT' : "Bhutan",
            'BO' : "Bolivia",
            'BQ' : "Bonaire, Saint Eustatius and Saba",
            'BA' : "Bosnia and Herzegovina",
            'BW' : "Botswana",
            'BR' : "Brazil",
            'IO' : "British Indian Ocean Territory",
            'BN' : "Brunei Darussalam",
            'BG' : "Bulgaria",
            'BF' : "Burkina Faso",
            'BI' : "Burundi",
            'KH' : "Cambodia",
            'CM' : "Cameroon",
            'CA' : "Canada",
            'CV' : "Cape Verde",
            'KY' : "Cayman Islands",
            'CF' : "Central African Republic",
            'TD' : "Chad",
            'CL' : "Chile",
            'CN' : "China",
            'CX' : "Christmas Island",
            'CC' : "Cocos (Keeling) Islands",
            'CO' : "Colombia",
            'KM' : "Comoros",
            'CG' : "Congo",
            'CD' : "Congo, The Democratic Republic of the",
            'CK' : "Cook Islands",
            'CR' : "Costa Rica",
            'CI' : "Cote D'Ivoire",
            'HR' : "Croatia",
            'CU' : "Cuba",
            'CW' : "Curacao",
            'CY' : "Cyprus",
            'CZ' : "Czech Republic",
            'DK' : "Denmark",
            'DJ' : "Djibouti",
            'DM' : "Dominica",
            'DO' : "Dominican Republic",
            'EC' : "Ecuador",
            'EG' : "Egypt",
            'SV' : "El Salvador",
            'GQ' : "Equatorial Guinea",
            'ER' : "Eritrea",
            'EE' : "Estonia",
            'ET' : "Ethiopia",
            'EU' : "Europe",
            'FK' : "Falkland Islands (Malvinas)",
            'FO' : "Faroe Islands",
            'FJ' : "Fiji",
            'FI' : "Finland",
            'FR' : "France",
            'GF' : "French Guiana",
            'PF' : "French Polynesia",
            'TF' : "French Southern Territories",
            'GA' : "Gabon",
            'GM' : "Gambia",
            'GE' : "Georgia",
            'DE' : "Germany",
            'GH' : "Ghana",
            'GI' : "Gibraltar",
            'GR' : "Greece",
            'GL' : "Greenland",
            'GD' : "Grenada",
            'GP' : "Guadeloupe",
            'GU' : "Guam",
            'GT' : "Guatemala",
            'GG' : "Guernsey",
            'GN' : "Guinea",
            'GW' : "Guinea-Bissau",
            'GY' : "Guyana",
            'HT' : "Haiti",
            'VA' : "Holy See (Vatican City State)",
            'HN' : "Honduras",
            'HK' : "Hong Kong",
            'HU' : "Hungary",
            'IS' : "Iceland",
            'IN' : "India",
            'ID' : "Indonesia",
            'IR' : "Iran, Islamic Republic of",
            'IQ' : "Iraq",
            'IE' : "Ireland",
            'IM' : "Isle of Man",
            'IL' : "Israel",
            'IT' : "Italy",
            'JM' : "Jamaica",
            'JP' : "Japan",
            'JE' : "Jersey",
            'JO' : "Jordan",
            'KZ' : "Kazakhstan",
            'KE' : "Kenya",
            'KI' : "Kiribati",
            'KP' : "Democratic People's Republic of Korea",
            'KR' : "Republic of Korea",
            'KW' : "Kuwait",
            'KG' : "Kyrgyzstan",
            'LA' : "Lao People's Democratic Republic",
            'LV' : "Latvia",
            'LB' : "Lebanon",
            'LS' : "Lesotho",
            'LR' : "Liberia",
            'LY' : "Libya",
            'LI' : "Liechtenstein",
            'LT' : "Lithuania",
            'LU' : "Luxembourg",
            'MO' : "Macau",
            'MK' : "Macedonia",
            'MG' : "Madagascar",
            'MW' : "Malawi",
            'MY' : "Malaysia",
            'MV' : "Maldives",
            'ML' : "Mali",
            'MT' : "Malta",
            'MH' : "Marshall Islands",
            'MQ' : "Martinique",
            'MR' : "Mauritania",
            'MU' : "Mauritius",
            'YT' : "Mayotte",
            'MX' : "Mexico",
            'FM' : "Micronesia, Federated States of",
            'MD' : "Moldova, Republic of",
            'MC' : "Monaco",
            'MN' : "Mongolia",
            'ME' : "Montenegro",
            'MS' : "Montserrat",
            'MA' : "Morocco",
            'MZ' : "Mozambique",
            'MM' : "Myanmar",
            'NA' : "Namibia",
            'NR' : "Nauru",
            'NP' : "Nepal",
            'NL' : "Netherlands",
            'NC' : "New Caledonia",
            'NZ' : "New Zealand",
            'NI' : "Nicaragua",
            'NE' : "Niger",
            'NG' : "Nigeria",
            'NU' : "Niue",
            'NF' : "Norfolk Island",
            'MP' : "Northern Mariana Islands",
            'NO' : "Norway",
            'OM' : "Oman",
            'PK' : "Pakistan",
            'PW' : "Palau",
            'PS' : "Palestinian Territory",
            'PA' : "Panama",
            'PG' : "Papua New Guinea",
            'PY' : "Paraguay",
            'PE' : "Peru",
            'PH' : "Philippines",
            'PN' : "Pitcairn Islands",
            'PL' : "Poland",
            'PT' : "Portugal",
            'PR' : "Puerto Rico",
            'QA' : "Qatar",
            'RE' : "Reunion",
            'RO' : "Romania",
            'RU' : "Russian Federation",
            'RW' : "Rwanda",
            'BL' : "Saint Barthelemy",
            'SH' : "Saint Helena",
            'KN' : "Saint Kitts and Nevis",
            'LC' : "Saint Lucia",
            'MF' : "Saint Martin",
            'PM' : "Saint Pierre and Miquelon",
            'VC' : "Saint Vincent and the Grenadines",
            'WS' : "Samoa",
            'SM' : "San Marino",
            'ST' : "Sao Tome and Principe",
            'SA' : "Saudi Arabia",
            'SN' : "Senegal",
            'RS' : "Serbia",
            'SC' : "Seychelles",
            'SL' : "Sierra Leone",
            'SG' : "Singapore",
            'SX' : "Sint Maarten",
            'SK' : "Slovakia",
            'SI' : "Slovenia",
            'SB' : "Solomon Islands",
            'SO' : "Somalia",
            'ZA' : "South Africa",
            'GS' : "South Georgia and the South Sandwich Islands",
            'SS' : "South Sudan",
            'ES' : "Spain",
            'LK' : "Sri Lanka",
            'SD' : "Sudan",
            'SR' : "Suriname",
            'SJ' : "Svalbard and Jan Mayen",
            'SZ' : "Swaziland",
            'SE' : "Sweden",
            'CH' : "Switzerland",
            'SY' : "Syrian Arab Republic",
            'TW' : "Taiwan",
            'TJ' : "Tajikistan",
            'TZ' : "Tanzania, United Republic of",
            'TH' : "Thailand",
            'TL' : "Timor-Leste",
            'TG' : "Togo",
            'TK' : "Tokelau",
            'TO' : "Tonga",
            'TT' : "Trinidad and Tobago",
            'TN' : "Tunisia",
            'TR' : "Turkey",
            'TM' : "Turkmenistan",
            'TC' : "Turks and Caicos Islands",
            'TV' : "Tuvalu",
            'UG' : "Uganda",
            'UA' : "Ukraine",
            'AE' : "United Arab Emirates",
            'GB' : "United Kingdom",
            'UK' : "England",
            'US' : "United States",
            'UM' : "United States Minor Outlying Islands",
            'UY' : "Uruguay",
            'UZ' : "Uzbekistan",
            'VU' : "Vanuatu",
            'VE' : "Venezuela",
            'VN' : "Vietnam",
            'VG' : "Virgin Islands, British",
            'VI' : "Virgin Islands, U.S.",
            'WF' : "Wallis and Futuna",
            'YE' : "Yemen",
            'ZM' : "Zambia",
            'ZW' : "Zimbabwe"}
        country_code = 'US'
        for code, name in country_def.items():
            if (country.replace(' ', '').lower() == name.replace(' ', '')):
                country_code = code
        return country_code