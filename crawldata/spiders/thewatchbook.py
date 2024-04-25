import time

import scrapy,json,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'thewatchbook'
    DATASET={}
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':1}
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
                if str(REF['model_number']).strip()!="":
                    REF['manufacturer']=str(REF['manufacturer']).upper()
                    REF['model_number']=str(REF['model_number']).upper()
                    if not REF['manufacturer'] in self.DATASET:
                        self.DATASET[REF['manufacturer']]=[]
                    if not REF['model_number'] in self.DATASET[REF['manufacturer']]:
                        self.DATASET[REF['manufacturer']].append(REF['model_number'])
        url='https://www.thewatchbook.com/page-data/sq/d/105469581.json'
        yield scrapy.Request(url,callback=self.parse)
    def parse(self, response):
        Data=json.loads(response.text)
        for row in Data['data']['allWatches']['nodes']:
            url='https://www.thewatchbook.com/page-data/watch/'+self.Get_URL(row['name'])+'-'+row['_id']+'/page-data.json'
            yield scrapy.Request(url,callback=self.parse_content,meta={'ROW':row})
    def parse_content(self,response):
        ROW=response.meta['ROW']
        Data=json.loads(response.text)
        row=Data['result']['data']['watches']
        pro=Data['result']['data']['allTerms']['nodes']
        TERM={}
        for rs in pro:
            TERM[rs['_id']]=rs['name']
        BRAND=str(TERM[row['brand'][0]]).upper()
        REF_NUM=(str(row['reference']).split('-')[0]).upper()
        if BRAND in self.DATASET and REF_NUM in self.DATASET[BRAND]:
            item={}
            item.update(ITEM_DATA)
            item['listing_uuid']=row['_id']
            item['brand']=TERM[row['brand'][0]]
            item['listing_url']='https://www.thewatchbook.com/watch/'+self.Get_URL(row['name'])+'-'+row['_id']
            if item['listing_url'] in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {item["listing_url"]}')
                return
            update_scrapped_urls(self.name, item['listing_url'], self.logger)
            self.done_urls.append(item['listing_url'])

            item['ref_number']=str(row['reference']).split('-')[0]
            item['production_year']=row.get('year_launched','')
            try:
                item['dial_color']=TERM[row['dial'][0]]
            except:
                pass
            TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
            for Type in TYPES:
                if Type in row['name'] and item['bracelet_type']=='':
                    item['bracelet_type']=Type
            item['case_size']=str(row['case_diameter'])+'mm'
            material=[]
            for rs in row['case_material']:
                if rs in TERM:
                    material.append(TERM[rs])
            item['material']=', '.join(material)
            item['scraping_date']=self.DATE_CRAWL
            item['price']=row.get('market_price','')
            item['model']=row.get('model','')
            item['brand']=str(item['brand']).title()
            #Scraped data
            DATASET={}
            metadata={}
            metadata['marketplace']=str(self.name).title()
            metadata['search_query']=str(BRAND).title()+' '+REF_NUM
            metadata['search_type']='listing'
            metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
            metadata['event_time']=''
            metadata['event_id']=str(item['listing_uuid'])
            metadata['event_url']=item['listing_url']
            scraped_data={}
            scraped_data['price']=item['price']
            scraped_data['title']=row['name']
            scraped_data['sub_title']=''
            scraped_data['description']=row.get('description','')
            scraped_data['manufacturer']=item['brand']
            scraped_data['model']=item['model']
            scraped_data['reference_number']=item['ref_number']
            scraped_data['case_material']=item['material']
            scraped_data['bracelet_material']=row.get('bracelet_material','')
            scraped_data['dial_color']=item['dial_color']
            scraped_data['dial_indicies']=item['numerals']
            scraped_data['bezel']=row.get('bezel_material','')
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
        
    def Get_URL(self,xau):
        xau=str(xau).lower()
        KQ=re.sub(r"([^a-z0-9- ])","", str(xau).strip())
        KQ=re.sub(r"([^a-z0-9])","-", str(KQ).strip())
        return KQ
