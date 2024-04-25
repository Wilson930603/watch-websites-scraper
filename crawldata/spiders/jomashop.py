import time

import scrapy,json,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'jomashop'
    url='https://www.jomashop.com/'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    #custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    SHA256=''
    def start_requests(self):
        self.done_urls = get_scrapped_urls(self.name, self.logger)

        yield scrapy.Request('http://dev1.crawler.pro.vn/jomashop_SHA256.txt',callback=self.get_sha)
    def get_sha(self,response):
        self.SHA256=str(response.text).strip()
        for _ in range(360):
            try:
                manufacturers_response = requests.get(CONFIGS['manufacturer_api'])
                Data=json.loads(manufacturers_response.text)
                break
            except Exception as e:
                # log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                #                   CONFIGS['manufacturer_api'],
                #                   'HIGH', 'MANUFACTURERS_API_ISSUE',
                #                   'Fail To Get Response From Manufacturers API, Will Retry After 10 Seconds')
                self.logger.info('Fail To Get Response From Manufacturers API, Will Retry After 10 Seconds')
                time.sleep(9)
                continue
        else:
            log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                              CONFIGS['manufacturer_api'],
                              'CRITICAL', 'MANUFACTURERS_API_ISSUE',
                              'Fail To Get Response From Manufacturers API Over The Last Hour')
            Data = []

        for BRAND in Data:
            url=f"{CONFIGS['brand_api']}?manufacturer={quote(BRAND)}"
            for _ in range(360):
                try:
                    brand_response = requests.get(url)
                    brand_data = json.loads(brand_response.text)
                    for REF in brand_data:
                        KEY = [REF['manufacturer'], REF['model_name'], REF['model_number']]
                    break
                except Exception as e:
                    # log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                    #                   url, 'HIGH', 'MANUFACTURERS_API_ISSUE',
                    #                   f'Models request failed of manufacturer {BRAND} in spider {self.name},'
                    #                   f' Will retry every 10 sec for the coming hour')
                    self.logger.info(f'Models request failed of manufacturer {BRAND} in spider {self.name},'
                                     f' Will retry every 10 sec for the coming hour')
                    time.sleep(9)
                    continue
            else:
                log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                                  url, 'CRITICAL', 'MANUFACTURERS_API_ISSUE',
                                  f'Models request failed of manufacturer {BRAND} in spider {self.name},'
                                  f' Over The last Hour')
                brand_data = []

            for REF in brand_data:
                KEY=[REF['manufacturer'],REF['model_name'],REF['model_number']]
                SEARCH=' '.join(KEY)
                page=0
                data = {"requests":[{"indexName":"productionM2_default_products","params":"clickAnalytics=true&facets=%5B%22has_coupon%22%2C%22get_it_fast%22%2C%22price.USD.default%22%2C%22series%22%2C%22manufacturer%22%2C%22department%22%2C%22subtype%22%2C%22shoe_style%22%2C%22handbag_style%22%2C%22general_size%22%2C%22frame_style%22%2C%22color%22%2C%22gender%22%2C%22movement%22%2C%22item_condition%22%2C%22name_wout_brand%22%2C%22model_id%22%2C%22description%22%2C%22sku%22%2C%22handbag_material%22%2C%22image_label%22%2C%22band_material%22%2C%22shoe_vamp%22%2C%22is_preowned%22%2C%22msrp_display_actual_price_type%22%2C%22name%22%2C%22has_small_image%22%2C%22promotext_value%22%2C%22promotext_code%22%2C%22promotext_type%22%2C%22item_variation%22%5D&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=24&maxValuesPerFacet=999&page="+str(page)+"&query="+SEARCH+"&tagFilters="}]}
                url='https://3pg985ekk8-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.33.0)%3B%20Browser%20(lite)&x-algolia-application-id=3PG985EKK8&x-algolia-api-key=ad64fcbef5f78b449be8563c3e14d16a'
                yield scrapy.Request(url, method="POST", body=json.dumps(data), callback=self.parse, meta={'REF':REF,'page':page,'SEARCH':SEARCH}, dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        Data=json.loads(response.text)
        if 'results' in Data:
            for rows in Data['results']:
                for row in rows['hits']:
                    ID=str(row['url']).split('/')
                    ID=str(ID[len(ID)-1]).split('.html')[0]
                    url='https://www.jomashop.com/graphql?operationName=productDetail&variables=%7B%22urlKey%22%3A%22'+ID+'%22%2C%22onServer%22%3Atrue%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22'+self.SHA256+'%22%7D%7D'

                    department = row.get('department', '')
                    subtype = row.get('subtype', '')

                    if 'watch' not in (department + subtype).lower():
                        continue

                    if row['url'] in self.done_urls:
                        self.logger.info(f'URL SCRAPPED BEFORE: {row["url"]}')
                        continue

                    update_scrapped_urls(self.name, row['url'], self.logger)
                    self.done_urls.append(row['url'])
                    yield scrapy.Request(url, callback=self.parse_content, meta={'REF':REF,'ROW':row}, dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
                if rows['page']<rows['nbPages']:
                    page+=1
                    data = {"requests":[{"indexName":"productionM2_default_products","params":"clickAnalytics=true&facets=%5B%22has_coupon%22%2C%22get_it_fast%22%2C%22price.USD.default%22%2C%22series%22%2C%22manufacturer%22%2C%22department%22%2C%22subtype%22%2C%22shoe_style%22%2C%22handbag_style%22%2C%22general_size%22%2C%22frame_style%22%2C%22color%22%2C%22gender%22%2C%22movement%22%2C%22item_condition%22%2C%22name_wout_brand%22%2C%22model_id%22%2C%22description%22%2C%22sku%22%2C%22handbag_material%22%2C%22image_label%22%2C%22band_material%22%2C%22shoe_vamp%22%2C%22is_preowned%22%2C%22msrp_display_actual_price_type%22%2C%22name%22%2C%22has_small_image%22%2C%22promotext_value%22%2C%22promotext_code%22%2C%22promotext_type%22%2C%22item_variation%22%5D&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=24&maxValuesPerFacet=999&page="+str(page)+"&query="+SEARCH+"&tagFilters="}]}
                    url='https://3pg985ekk8-2.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.33.0)%3B%20Browser%20(lite)&x-algolia-application-id=3PG985EKK8&x-algolia-api-key=ad64fcbef5f78b449be8563c3e14d16a'
                    yield scrapy.Request(url, method="POST", body=json.dumps(data), callback=self.parse, meta={'REF':REF,'page':page,'SEARCH':SEARCH}, dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)

    def parse_content(self,response):
        REF=response.meta['REF']
        ROW=response.meta['ROW']
        Data=json.loads(response.text)
        if 'data' in Data:
            ROWS=Data['data']['productDetail']['items']
            for rows in ROWS:
                DATA=[]
                if 'items' in rows:
                    for RS in rows['items']:
                        DATA.append(RS['product'])
                        pass
                else:
                    DATA.append(rows)
                for row in DATA:
                    if row['stock_status']=='IN_STOCK':
                        IT={}
                        for rs in row['moredetails']['more_details']:
                            for rcs in rs['group_attributes']:
                                IT[rcs['attribute_id']]=rcs['attribute_value']
                        item={}
                        item.update(ITEM_DATA)
                        item['listing_uuid']=row['id']
                        try:
                            item['brand']=row['brand_name']
                        except:
                            item['brand']=''
                        item['listing_url']=self.url+row['url_key']+'.html'
                        item['ref_number']=row.get('model_id','')
                        item['production_year']=''
                        try:
                            item['dial_color']=IT['dial_color']
                        except:
                            item['dial_color']=''
                        item['bracelet_type']=''
                        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
                        material=[]
                        Box=False
                        Papers=False
                        condition=''
                        for Type in TYPES:
                            for k,v in IT.items():
                                if v and Type in v and item['bracelet_type']=='':
                                    item['bracelet_type']=Type
                        for k,v in IT.items():
                            if '_material' in k and not v in material:
                                material.append(v)
                            if '_box' in k:
                                if v=='Yes':
                                    Box=True
                            if '_papers' in k:
                                if v=='Yes':
                                    Papers=True
                            if '_item_condition' in k:
                                condition=v
                            if '_year' in k and len(Get_Number(v))==4:
                                item['production_year']=Get_Number(v)
                        try:
                            item['case_size']=IT['case_diameter']
                        except:
                            item['case_size']=''
                        try:
                            bezel=[]
                            bezel.append(IT['bezel_material'])
                            bezel.append(IT['bezel_color'])
                            bezel.append(IT['bezel_material_only'])
                            item['bezel']=', '.join(bezel)
                        except:
                            item['bezel']=''
                        item['material']=', '.join(material)
                        item['condition']=condition
                        if row['is_preowned']==1:
                            item['new_preowned']='Preowned'
                        else:
                            item['new_preowned']='NEW'
                        item['complete_set']='FALSE'
                        if row['is_preowned']==0:
                            item['complete_set']='TRUE'
                        else:
                            if Box==True and Papers==True:
                                item['complete_set']='TRUE'
                            elif Box==True:
                                item['complete_set']='box'
                            elif Papers==True:
                                item['complete_set']='papers'
                        item['scraping_date']=self.DATE_CRAWL
                        try:
                            item['price']=row['price_range']['minimum_price']['plp_price']['now_price']
                        except:
                            item['price']=row['price_range']['minimum_price']['final_price']['value']
                        item['sale_date']=''
                        item['model']=row.get('model_id','')
                        item['brand']=str(item['brand']).title()
                        #Scraped data
                        DATASET={}
                        metadata={}
                        metadata['marketplace']=str(self.name).title()
                        metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
                        metadata['search_type']='listing'
                        metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
                        metadata['event_time']=''
                        metadata['event_id']=str(item['listing_uuid'])
                        metadata['event_url']=item['listing_url']
                        scraped_data={}
                        scraped_data['price']=item['price']
                        scraped_data['title']=row['name']
                        scraped_data['sub_title']=row['name_wout_brand']
                        scraped_data['description']=ROW['description']
                        scraped_data['manufacturer']=item['brand']
                        scraped_data['model']=item['model']
                        scraped_data['reference_number']=item['ref_number']
                        scraped_data['case_material']=IT.get('case_material','')
                        scraped_data['bracelet_material']=IT.get('bracelet_material','')
                        scraped_data['dial_color']=item['dial_color']
                        scraped_data['dial_indicies']=item['numerals']
                        scraped_data['bezel']=IT.get('bezel_material','')
                        scraped_data['case_size']=item['case_size']
                        scraped_data['condition']=item['condition']
                        if item['complete_set']=='TRUE' or item['complete_set']=='box':
                            scraped_data['box']=True
                        else:
                            scraped_data['box']=False
                        if item['complete_set']=='TRUE' or item['complete_set']=='papers':
                            scraped_data['papers']=True
                        else:
                            scraped_data['papers']=True
                        scraped_data['production_year']=item['production_year']
                        scraped_data['miscellaneous']={}
                        (scraped_data['miscellaneous']).update(ROW)
                        (scraped_data['miscellaneous']).update(row)
                        DATASET['metadata']=metadata
                        DATASET['scraped_data']=scraped_data
                        yield(DATASET)
