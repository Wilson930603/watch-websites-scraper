import time

import scrapy,json,os,platform,html
from crawldata.functions import *
from datetime import datetime
from crawldata.settings import *
from urllib.parse import quote
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'bobswatches'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    custom_settings={'DOWNLOAD_DELAY':1.5,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
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
                url='https://rbeja3.a.searchspring.io/api/search/search.json?ajaxCatalog=v3&resultsFormat=native&siteId=rbeja3&bgfilter.ss_availability=in%20stock&bgfilter.ss_is_accessory=0&q='+SEARCH+'&page='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        Data=json.loads(response.text)
        if Data['merchandising']['redirect']!='':
            ID=str(Data['merchandising']['redirect']).split('/')
            SEARCH=ID[len(ID)-1]
            page=1
            url='https://rbeja3.a.searchspring.io/api/search/search.json?ajaxCatalog=v3&resultsFormat=native&siteId=rbeja3&bgfilter.ss_availability=in%20stock&bgfilter.ss_is_accessory=0&q='+SEARCH+'&page='+str(page)
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
        else:
            for row in Data['results']:
                if row['url'] in self.done_urls:
                    self.logger.info(f'URL SCRAPPED BEFORE: {row["url"]}')
                    continue
                update_scrapped_urls(self.name, row['url'], self.logger)
                self.done_urls.append(row['url'])
                yield scrapy.Request(row['url'],callback=self.parse_content,meta={'REF':REF,'ROW':row},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
            if Data['pagination']['nextPage']>0:
                url='https://rbeja3.a.searchspring.io/api/search/search.json?ajaxCatalog=v3&resultsFormat=native&siteId=rbeja3&bgfilter.ss_availability=in%20stock&bgfilter.ss_is_accessory=0&q='+SEARCH+'&page='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_content(self,response):
        REF=response.meta['REF']
        ROW=response.meta['ROW']
        Data=response.xpath('//div[@id="panel-collapseProductDetail"]//tr')
        IT={}
        for row in Data:
            TITLE=row.xpath('./td[1]//text()').get()
            VAL=row.xpath('./td[2]//text()').get()
            IT[str(TITLE).replace(':', '').strip().lower()]=str(VAL).strip()
        # Old data
        item={}
        item.update(ITEM_DATA)
        item['KEY_']=Get_KEY(REF)+'_'+ROW['sku']
        item['listing_uuid']=ROW['sku']
        item['brand']=ROW['brand']
        item['listing_url']=response.url
        item['ref_number']=ROW['ss_personalization_categories'][-1]
        if 'serial/year' in IT:
            YEAR=str(IT['serial/year']).split()
            YEAR=YEAR[len(YEAR)-1]
            if len(Get_Number(YEAR))==4 and len(Get_Number_Only(YEAR))==4:
                item['production_year']=Get_Number(YEAR)
        if item['production_year']=='':
            if 'produced' in IT:
                Produced=str(IT['produced']).strip().split()
                YEAR=Produced[len(Produced)-1]
                if YEAR==Get_Number(YEAR):
                    item['production_year']=YEAR
        item['dial_color']=''
        if 'dial' in IT:
            color=(str(IT['dial']).split('w/')[0]).lower()
        if color in COLORS:
            item['dial_color']=color.title()
        else:
            TXT=ROW['name']
            if 'case' in IT:
                TXT+=' '+IT['case']
            if 'dial' in IT:
                TXT+=' '+IT['dial']
            TXT=(str(TXT).lower()).split()
            for Txt in TXT:
                if str(Txt).lower() in COLORS and item['dial_color']=='':
                    item['dial_color']=str(Txt).title()
        item['bracelet_type']=''
        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
        preowned='NEW'
        PRE=['Excellent','Vintage','Good','Very Good']
        for Type in TYPES:
            for rs in ROW.values():
                if Type in str(rs) and item['bracelet_type']=='':
                    item['bracelet_type']=Type
            for rs in IT.values():
                if Type in str(rs) and item['bracelet_type']=='':
                    item['bracelet_type']=Type
        item['case_size']=''
        if 'case' in IT:
            TXT=str(IT['case']).split('w/')[0]
            if '(' in TXT:
                SIZE=str(TXT).split('(')[1].split(')')[0]
                item['case_size']=SIZE
                item['material']=str(TXT).split('(')[0]
        if 'condition' in IT:
            item['condition']=IT['condition']
            if item['condition'] in PRE:
                preowned='Preowned'
        item['new_preowned']=preowned
        item['complete_set']='FALSE'
        item['scraping_date']=self.DATE_CRAWL
        item['price']=ROW['price']
        Box=False
        Papers=False
        if 'box & papers' in IT:
            if 'box' in IT['box & papers']:
                Box=True
            if 'papers' in IT['box & papers']:
                Papers=True
        if Box==True and Papers==True:
            item['complete set']='TRUE'
        elif Box==True:
            item['complete set']='box'
        elif Papers==True:
            item['complete set']='papers'
        item['model']=IT.get('model name/number','')
        item['bezel']=''
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
        metadata['event_url']=response.url
        DATASET['metadata']=metadata
        scraped_data={}
        scraped_data['price']=item['price']
        scraped_data['title']=html.unescape(ROW['name'])
        scraped_data['sub_title']=ROW['custom_field_2']+' '+ROW['custom_field_3']
        scraped_data['description']=cleanhtml(response.xpath('//div[@itemprop="description"]').get())
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
        scraped_data['box']=Box
        scraped_data['papers']=Papers
        scraped_data['production_year']=item['production_year']
        scraped_data['miscellaneous']={}
        (scraped_data['miscellaneous']).update(ROW)
        (scraped_data['miscellaneous']).update(IT)
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
