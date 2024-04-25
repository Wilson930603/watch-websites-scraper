import time

import scrapy,json,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'watchesworld'
    domain='https://www.watchesworld.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    # custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','X-Requested-With': 'XMLHttpRequest','Origin': 'https://www.watchesworld.com','Alt-Used': 'www.watchesworld.com','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin'}
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
                # json_data = {"action": "elastic_search_products","keyword": SEARCH,"sort": "","offset": "0"}
                # url='https://www.watchesworld.com/wp-admin/admin-ajax.php'
                # yield scrapy.FormRequest(url,formdata=json_data,callback=self.parse,meta={'json_data':json_data,'REF':REF,'SEARCH':SEARCH,'TOTAL':0},headers=self.headers,dont_filter=True)
                page = 1
                url = f'https://www.watchesworld.com/search-page/?keyword={quote(SEARCH)}&showpage={page}'
                yield scrapy.Request(url, callback=self.parse, meta={'page':page,'REF':REF,'SEARCH':SEARCH,'TOTAL':0},headers=self.headers, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        TOTAL=response.meta['TOTAL']
        # json_data=response.meta['json_data']
        page=response.meta['page']
        Data=response.xpath('//div[@class="productBox"]')
        for row in Data:
            TOTAL+=1
            JSON=row.xpath('.//script/text()').get()
            data=json.loads(JSON)
            LS=['sku','name','material','model','releaseDate','size','description']
            ITEM={}
            for ls in LS:
                ITEM[str(ls).lower()]=data.get(ls,'')
            ITEM['brand']=data['brand'].get('name','')
            ITEM['price']=data['offers'].get('price','')
            ITEM['url']=data['offers'].get('url','')
            if ITEM['url'] in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {ITEM["url"]}')
                continue
            update_scrapped_urls(self.name, ITEM['url'], self.logger)
            self.done_urls.append(ITEM['url'])
            yield scrapy.Request(ITEM['url'],callback=self.parse_data,meta={'REF':REF,'SEARCH':SEARCH,'ITEM':ITEM,'data':data},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        # Next page
        if len(Data)>0:
            # json_data['offset']=str(int(json_data['offset'])+len(Data))
            # yield scrapy.FormRequest(response.url,formdata=json_data,callback=self.parse,meta={'json_data':json_data,'REF':REF,'SEARCH':SEARCH,'TOTAL':TOTAL},headers=self.headers,dont_filter=True)
            page += 1
            url = f'https://www.watchesworld.com/search-page/?keyword={quote(SEARCH)}&showpage={page}'
            yield scrapy.Request(url, callback=self.parse, meta={'page':page,'REF':REF,'SEARCH':SEARCH,'TOTAL':0},headers=self.headers, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)

    def parse_data(self,response):
        REF=response.meta['REF']
        data=response.meta['data']
        SEARCH=response.meta['SEARCH']
        ITEMS=response.meta['ITEM']
        BRANDS=(str(REF).split('~')[0]).split('|')
        ITEM={}
        ITEM['brand']=str(response.xpath('//h2[contains(@class,"brand-name")]/text()').get()).strip()
        ITEM['name']=response.xpath('//h1[@class="product-name"]/text()').get()
        Data=response.xpath('//div[@id="product_offers"]//div[contains(@class,"attribute ")]')
        for row in Data:
            TITLE=row.xpath('./label/text()').get()
            VAL=row.xpath('./div/text()').get()
            ITEM[Get_Key_Str(TITLE)]=VAL
        Data=response.xpath('//div[@class="attributes-list"]//div[contains(@class,"group-items")]/div[contains(@class,"item ")]')
        for row in Data:
            TITLE=row.xpath('./div[@class="name-attribute"]//text()').get()
            VAL=row.xpath('./div[@class="attribute-value"]//text()').get()
            ITEM[Get_Key_Str(TITLE)]=VAL
        for k,v in ITEM.items():
            if not k in ITEMS or ITEMS[k]=='':
                ITEMS[k]=v
        #yield(ITEMS)

        item={}
        item.update(ITEM_DATA)
        item['listing uuid']=ITEMS['sku']
        item['brand']=ITEMS.get('brand','')
        item['listing_url']=ITEMS.get('url','')
        item['ref_number']=response.xpath('//div[contains(@class,"basic-product-info")]//div[@data-ref]/@data-ref').get()
        item['production_year']=ITEMS.get('releasedate','')
        item['dial_color']=ITEMS.get('dial','')
        item['case_size']=ITEMS.get('size','')
        item['material']=ITEMS.get('material','')
        item['numerals']=ITEMS.get('dialnumerals')
        item['condition']=ITEMS.get('condition','')
        if item['condition']=='New':
            item['new_preowned']='New'
        else:
            item['new_preowned']='Preowned'
        item['complete_set']='FALSE'
        item['scraping_date']=''
        item['price']=ITEMS.get('price','')
        item['model']=ITEMS.get('model','')
        item['bezel']=ITEMS.get('bezelmaterial','')

        NAME=ITEMS.get('name','')+' '+ITEMS.get('description','')
        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
        for Type in TYPES:
            if Type in NAME and item['bracelet_type']=='':
                item['bracelet_type']=Type
        Box=False
        Papers=False
        if ITEMS.get('box','')=='Original Box':
            Box=True
        if ITEMS.get('paper','')=='Original papers':
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
        scraped_data['title']=ITEMS.get('name','')
        scraped_data['sub_title']=''
        scraped_data['description']=ITEMS.get('description','')
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=data.get('bracelet','')
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
        (scraped_data['miscellaneous']).update(ITEMS)
        (scraped_data['miscellaneous']).update(data)
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
