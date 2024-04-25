import time

import scrapy,json,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3

class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'loupethis'
    domain='https://www.loupethis.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    #custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a')+'.log'}
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
                url='https://api.loupethis.com/api/v1/auctions?brand_ids=&status=active&sort_by=listed_at_desc&per_page=24&query='+SEARCH+'&page=1'
                yield scrapy.Request(url,callback=self.parse_list,meta={'REF':REF,'TYPE':'oracle'},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
                url='https://api.loupethis.com/api/v1/auctions?brand_ids=&status=closed&sort_by=listed_at_desc&per_page=24&query='+SEARCH+'&page=1'
                yield scrapy.Request(url,callback=self.parse_list,meta={'REF':REF,'TYPE':'sale'},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_list(self,response):
        REF=response.meta['REF']
        TYPE=response.meta['TYPE']
        Data=json.loads(response.text)
        for row in Data['data']:
            url=self.domain+"/"+row['type']+'/'+row['attributes']['slug']
            if url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {url}')
                continue
            update_scrapped_urls(self.name, url, self.logger)
            self.done_urls.append(url)
            yield scrapy.Request(url,callback=self.parse_content,meta={'REF':REF,'TYPE':TYPE,'ROW':row},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        if Data['meta']['pagination']['next_page']:
            URL=str(response.url).split('&page=')
            url=URL[0]+'&page='+str(Data['meta']['pagination']['next_page'])
            yield scrapy.Request(url,callback=self.parse_list,meta={'REF':REF,'TYPE':TYPE},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_content(self,response):
        REF=response.meta['REF']
        TYPE=response.meta['TYPE']
        ROW=response.meta['ROW']
        BRANDS=[REF['manufacturer']]
        DESCS=response.xpath('//div[contains(@class,"PortableText")]/p/text()').getall()
        Data=response.xpath('//div[contains(@class,"TwoColumnTable__cells-container")]')
        for row in Data:
            TITLE=row.xpath('.//h4/text()').get()
            if TITLE:
                ROW[TITLE]=row.xpath('.//p/text()').get()
        
        #Scraped data
        DATASET={}
        metadata={}
        metadata['marketplace']=str(self.name).title()
        metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
        metadata['search_type']=TYPE
        metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
        if TYPE=='sale':
            metadata['event_time']=ROW['attributes']['ends_at']
        else:
            metadata['event_time']=''
        metadata['event_id']=ROW['id']
        metadata['event_url']=response.url
        DATASET['metadata']=metadata
        scraped_data={}
        if TYPE=='sale':
            scraped_data['price']=ROW['attributes']['sold_price_cents']
        else:
            scraped_data['price']=ROW['attributes']['current_bid_price_cents']

        # convert price from cents to dollar
        if scraped_data['price']:
            try:
                scraped_data['price'] = str(int(round(float(scraped_data['price']) / 100, 0)))
            except Exception as e:
                pass

        scraped_data['title']=ROW['attributes']['title']
        scraped_data['sub_title']=''
        scraped_data['description']=' \n '.join(DESCS)
        scraped_data['manufacturer']=ROW.get('Brand','')
        scraped_data['model']=ROW.get('Model','')
        scraped_data['reference_number']=ROW.get('Reference','')
        scraped_data['case_material']=ROW.get('Material','')
        scraped_data['bracelet_material']=ROW.get('Bracelet','')
        scraped_data['dial_color']=ROW.get('Dial Color','')
        scraped_data['dial_indicies']=''
        scraped_data['bezel']=''
        scraped_data['case_size']=ROW.get('Dimensions','')
        scraped_data['condition']=ROW.get('Condition','')
        Included=str(ROW.get('Included','')).lower()
        if 'box' in Included:
            scraped_data['box']=True
        else:
            scraped_data['box']=False
        if 'paper' in Included:
            scraped_data['papers']=True
        else:
            scraped_data['papers']=True
        scraped_data['production_year']=ROW.get('Year','')
        scraped_data['miscellaneous']={}
        (scraped_data['miscellaneous']).update(ROW)
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
