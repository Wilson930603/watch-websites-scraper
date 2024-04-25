import time

import scrapy,json,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'thewatchbox'
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
                url='https://www.thewatchbox.com/on/demandware.store/Sites-watchbox-us-Site/en_US/Search-UpdateJSON?sz=200000&q='+SEARCH+'&srule=best-matches'
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF},dont_filter=True)
    def parse(self, response):
        REF=response.meta['REF']
        HTML=str(response.text).split('"allProducts":')[1].split('}}},')[0]+'}}}'
        Data={}
        try:
            Data=eval(HTML)
        except:
            pass
        for ID,row in Data.items():
            url='https://www.thewatchbox.com/shop/'+ID+'.html'
            if url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {url}')
                continue
            update_scrapped_urls(self.name, url, self.logger)
            self.done_urls.append(url)
            yield scrapy.Request(url,callback=self.parse_content,meta={'REF':REF,'ROW':row})
    def parse_content(self,response):
        REF=response.meta['REF']
        ROW=response.meta['ROW']

        soldout=response.xpath('//div[@id="pdp-header"]//div[@class="pdp-soldout"]')
        if soldout:
            return

        Data=response.xpath('//div[@class="pdp-refine"]')
        for row in Data:
            DT=row.xpath('./span/text()').getall()
            if len(DT)>0:
                TITLE=Get_Key_Str((str(DT[0]).strip().replace(':', '')).lower())
                if len(DT)>1:
                    ROW[TITLE]=str(DT[1]).strip()
                else:
                    ROW[TITLE]=''
        DESC=response.xpath('//div[@itemprop="description"]/text()').get()
        item={}
        item.update(ITEM_DATA)
        item['listing_uuid']=ROW['prodid']
        item['brand']=ROW['brand']
        item['listing_url']=response.url
        item['ref_number']=ROW['ref']
        item['production_year']=str(ROW['year']).split('-')[0]
        item['dial_color']=ROW.get('dial_color','')
        item['bracelet_type']=''
        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
        for Type in TYPES:
            if Type in DESC and item['bracelet_type']=='':
                item['bracelet_type']=Type
        item['case_size']=ROW.get('case_size','')
        item['material']=ROW.get('case_material','')
        item['numerals']=''
        item['condition']=DESC
        if 'pre-owned' in str(DESC+' '+ROW['name']).lower():
            item['new_preowned']='Preowned'
        else:
            item['new_preowned']='NEW'
        item['complete_set']='FALSE'
        item['scraping_date']=self.DATE_CRAWL
        item['price']=ROW['price']['sale']
        item['bezel']=ROW.get('bezel','')
        item['model']=str(response.xpath('//span[@class="pdp-name"]/text()').get()).strip()
        Box=False
        Papers=False
        if 'box' in ROW and ROW['box']=='Yes':
            Box=True
        if 'papers' in ROW and ROW['papers']=='Yes':
            Papers=True
        if Box==True and Papers==True:
            item['complete_set']='TRUE'
        elif Box==True:
            item['complete_set']='box'
        elif Papers==True:
            item['complete_set']='papers'
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
        metadata['event_url']=item['listing_url']
        DATASET['metadata']=metadata
        scraped_data={}
        scraped_data['price']=item['price']
        scraped_data['title']=ROW['name']
        scraped_data['sub_title']=''
        scraped_data['description']=DESC
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=ROW.get('strap_bracelet_material','')
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
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
    
