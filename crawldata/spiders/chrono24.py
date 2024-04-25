import time

import scrapy,json,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'chrono24'
    domain='https://www.chrono24.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    # custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    def start_requests(self):
        self.done_urls = get_scrapped_urls(self.name, self.logger)

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
                url='https://www.chrono24.com/search/index.htm?dosearch=true&query='+SEARCH+'&showpage=1'
                yield scrapy.Request(url,callback=self.parse_list,meta={'REF':REF},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_list(self,response):
        # check proper crawlera response
        if 'User account suspended' in response.text:
            log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                              response.url,
                              'CRITICAL', 'RATE_LIMIT_REACHED',
                              f'Proxy API rate limit reached, spider {self.name} will close')
            self.crawler.engine.close_spider(self, 'API was suspended')
            return

        REF=response.meta['REF']
        Data=response.xpath('//div[@id="wt-watches"]/div/a[contains(@href,"--id")]/@href').getall()
        for row in Data:
            url=self.domain+row
            if url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {url}')
                continue
            self.done_urls.append(url)
            update_scrapped_urls(self.name, url, self.logger)
            yield scrapy.Request(url,callback=self.parse_content,meta={'REF':REF},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        next_page=response.xpath('//a[@class="paging-next"]/@href').get()
        if next_page:
            url='https://www.chrono24.com/search/'+next_page
            yield scrapy.Request(url,callback=self.parse_list,meta={'REF':REF},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_content(self,response):
        # check proper crawlera response
        if 'User account suspended' in response.text:
            log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                              response.url,
                              'CRITICAL', 'RATE_LIMIT_REACHED',
                              f'Proxy API rate limit reached, spider {self.name} will close')
            self.crawler.engine.close_spider(self, 'API was suspended')
            return

        REF=response.meta['REF']
        BRANDS=[REF['manufacturer']]
        ID = None
        if '"productID":"' in response.text:
            ID=str(response.text).split('"productID":"')[1].split('"')[0]
        elif '"dpWatchId":"' in response.text:
            ID=str(response.text).split('"dpWatchId":"')[1].split('"')[0]
        else:
            print('\n ================================================')
            print(response.url)

        if ID:
            item={}
            item.update(ITEM_DATA)
            item['listing_uuid']=ID
            item['brand']=''
            item['listing_url']=response.url

            try:
                PRICE=cleanhtml(response.xpath('//div[contains(@class,"detail-page-price")]//span[contains(@class,"price")]/text()').get())
                if len(Get_Number_Only(PRICE)) > 0:
                    item['price'] = PRICE
            except Exception as e:
                item['price'] = ''

            ITEM={}
            Data=response.xpath('//section[contains(@class,"details")]//tr')
            for rows in Data:
                try:
                    TD = rows.xpath('./td')
                    if len(TD) == 2:
                        TD1 = TD[0].xpath('./strong/text()').get()
                        if TD1:
                            TD2 = TD[1].xpath('.//text()').getall()
                            TD2_DATA = []
                            for i in range(len(TD2)):
                                TXT = str(TD2[i]).strip()
                                if TXT != '' and not 'function()' in TXT:
                                    TD2_DATA.append(TXT)
                            ITEM[((str(TD1).strip()).lower()).replace(' ', '_')] = ', '.join(TD2_DATA)
                except Exception:
                    continue

            item['ref_number']=ITEM.get('reference_number','')
            if 'case_diameter' in ITEM:
                item['case_size']=str(ITEM['case_diameter']).split(',')[0]
            if 'case_material' in ITEM:
                item['material']=ITEM['case_material']
            if 'dial_numerals' in ITEM:
                item['numerals']=ITEM['dial_numerals']
            if 'year_of_production' in ITEM:
                item['production_year']=Get_Number(ITEM['year_of_production'])
            for k,v in item.items():
                if v.strip() == '' and k in ITEM:
                    item[k]=ITEM[k]
            NAME=cleanhtml(response.xpath('//h1').get())
            TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
            for Type in TYPES:
                if NAME and Type in NAME and item['bracelet_type']=='':
                    item['bracelet_type']=Type
            if item['dial_color']=='' and 'dial' in ITEM:
                item['dial_color']=ITEM['dial']
            TXT=str(item['condition']).split(',')[0]
            if 'Unworn' in TXT or 'New' in TXT:
                item['new_preowned']='New'
            elif 'Very good' in TXT or 'Good' in TXT or 'whatever' in TXT:
                item['new_preowned']='Preowned'
            if 'scope_of_delivery' in ITEM:
                TXT=(str(ITEM['scope_of_delivery']).lower()).split(',')
                Box=False
                Papers=False
                for txt in TXT:
                    if 'box' in txt and (not 'no' in str(txt).split('box')[0]):
                        Box=True
                    if 'papers' in txt and (not 'no' in str(txt).split('papers')[0]):
                        Papers=True
                if Box==True and Papers==True:
                    item['complete_set']='TRUE'
                elif Box==True:
                    item['complete_set']='box'
                elif Papers==True:
                    item['complete_set']='papers'
            if item['brand']=='':
                for BR in BRANDS:
                    if BR in str(NAME).upper() and item['brand']=='':
                        item['brand']=str(BR).title()
            item['model']=ITEM.get('model','')
            item['bezel']=ITEM.get('bezel_material','')
            item['brand']=str(item['brand']).title()
            item['title']=str(response.xpath('//h1/text()').get()).strip()
            item['subtitle'] = str(response.xpath('//h1/div/text()').get()).strip() if response.xpath('//h1/div/text()') else ''
            item['description']=''
            #Scraped data
            DATASET={}
            metadata={}
            metadata['marketplace']=str(self.name).title()
            metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
            metadata['search_type']='listing'
            metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
            metadata['event_time']=item['sale_date']
            if metadata['event_time']!='':
                metadata['search_type']='sale'
            metadata['event_id']=str(item['listing_uuid'])
            metadata['event_url']=response.url
            DATASET['metadata']=metadata
            scraped_data={}
            scraped_data['price']=item['price']
            scraped_data['title']=item['title']
            scraped_data['sub_title']=item['subtitle']
            scraped_data['description']=item['description']
            scraped_data['manufacturer']=item['brand']
            scraped_data['model']=item['model']
            scraped_data['reference_number']=item['ref_number']
            scraped_data['case_material']=item.get('material','')
            scraped_data['bracelet_material']=ITEM.get('bracelet_material','')
            scraped_data['dial_color']=item['dial_color']
            scraped_data['dial_indicies']=item['numerals']
            scraped_data['bezel']=item['bezel']
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
            (scraped_data['miscellaneous']).update(ITEM)
            DATASET['scraped_data']=scraped_data
            ID=str(response.url).split('--id')[1].split('.')[0]
            url='https://www.chrono24.com/search/detail.htm?id='+ID+'&originalNotes='
            yield scrapy.Request(url,callback=self.parse_body,meta={'DATASET':DATASET},dont_filter=True, priority=10)
            
    def parse_body(self,response):
        DATASET=response.meta['DATASET']
        try:
            DATASET['scraped_data']['description'] = cleanhtml(str(response.text).split('<![CDATA[')[1].split(']]>')[0])
        except Exception:
            pass
        yield(DATASET)
