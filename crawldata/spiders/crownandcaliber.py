import time

import scrapy,json,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'crownandcaliber'
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
                page=1
                url='https://fvbnuy.a.searchspring.io/api/search/search.json?ajaxCatalog=v3&resultsFormat=native&siteId=fvbnuy&q='+SEARCH+'&page='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        Data=json.loads(response.text)
        for row in Data['results']:
            if row['url'] in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {row["url"]}')
                continue

            update_scrapped_urls(self.name, row['url'], self.logger)
            self.done_urls.append(row['url'])
            yield scrapy.Request(row['url'],callback=self.parse_contenthtml,meta={'REF':REF,'ROW':row},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        if Data['pagination']['nextPage']>0:
            url='https://fvbnuy.a.searchspring.io/api/search/search.json?ajaxCatalog=v3&resultsFormat=native&siteId=fvbnuy&q='+SEARCH+'&page='+str(page)
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_contenthtml(self,response):
        REF=response.meta['REF']
        ROW=response.meta['ROW']
        Data=response.xpath('//div[@class="prod-specs"]/div')
        IT={}
        for row in Data:
            TITLE=str(row.xpath('.//span[1]/text()').get()).strip()
            if TITLE[-1:]=='-':
                TITLE=((str(TITLE[:-1]).strip()).lower()).replace(' ', '-')
                VAL=str(row.xpath('.//span[@class="list-value"]/text()').get()).strip()
                IT[TITLE]=VAL
        yield scrapy.Request(ROW['url']+'.json',callback=self.parse_content,meta={'REF':REF,'ROW':ROW,'IT':IT},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
    def parse_content(self,response):
        REF=response.meta['REF']
        ROW=response.meta['ROW']
        IT=response.meta['IT']
        Data=json.loads(response.text)['product']
        tags=str(Data['tags']).split(',')
        for row in tags:
            if '::' in row:
                IT[(str(row).split('::')[0]).strip()]=str(row).split('::')[1]
            elif 'pre-owned' in row:
                IT[str(row).strip()]='Preowned'
            else:
                IT[str(row).strip()]=str(row).strip()
        DESC=Data['body_html']
        item={}
        item.update(ITEM_DATA)
        item['listing_uuid']=ROW['sku']
        item['brand']=ROW['brand']
        item['listing_url']=ROW['url']
        item['ref_number']=ROW.get('mfield_global_model_number','')
        item['production_year']=IT.get('year','')
        if item['production_year']=='':
            item['production_year']=IT.get('approximate_age','')
        if item['production_year']!='':
            if len(Get_Number_Only(item['production_year']))==4:
                item['production_year']=Get_Number_Only(item['production_year'])
            else:
                item['production_year']=''
        item['dial_color']=IT.get('dial_color','')
        item['bracelet_type']=''
        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
        preowned=''
        if 'pre-owned' in str(DESC):
            preowned='Preowned'
        for Type in TYPES:
            if Type in str(DESC) and item['bracelet_type']=='':
                item['bracelet_type']=Type
            for Val in IT.values():
                if Type in str(Val) and item['bracelet_type']=='':
                    item['bracelet_type']=Type
        item['case_size']=IT.get('case_size','')
        if item['case_size']!='' and not 'mm' in item['case_size']:
            item['case_size']+=' mm'
        item['material']=IT.get('case_materials','')
        item['numerals']=''
        item['condition']=IT.get('condition','')
        item['new_preowned']=IT.get('pre-owned',preowned)
        item['complete_set']=''
        item['scraping_date']=self.DATE_CRAWL
        item['price']=ROW['price']
        item['watches_db_foreign_key']=''
        item['sale_date']=''
        Box=False
        Papers=False
        item['bezel']=IT.get('bezel_materials','')
        item['model']=ROW.get('mfield_global_model_name','')
        if 'box' in IT:
            if IT['box']=='Yes':
                Box=True
        if 'papers' in IT:
            if IT['papers']=='Yes':
                Papers=True
        if Box==True and Papers==True:
            item['complete set']='TRUE'
        elif Box==True:
            item['complete set']='box'
        elif Papers==True:
            item['complete set']='papers'
        item['brand']=str(item['brand']).title()
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
        metadata['event_url']=str(response.url).replace('.json', '')
        DATASET['metadata']=metadata
        scraped_data={}
        scraped_data['price']=item['price']
        scraped_data['title']=ROW['name']
        scraped_data['sub_title']=IT.get('cn0','')
        scraped_data['description']=DESC
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=IT.get('bracelet','')
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
        (scraped_data['miscellaneous']).update(ROW)
        (scraped_data['miscellaneous']).update(IT)
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
        
