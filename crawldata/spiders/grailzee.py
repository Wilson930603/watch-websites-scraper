import time

import scrapy,json,re,string,dateparser,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'grailzee'
    domain='https://grailzee.com'
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
                url='https://grailzee.com/apps/auctioneer/api/auctions?q='+quote(SEARCH)+'&page=1'
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':1},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        page=response.meta['page']
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        Data=json.loads(response.text)
        for row in Data:
            url='https://grailzee.com/products/'+row['handle']
            if url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {url}')
                continue
            update_scrapped_urls(self.name, url, self.logger)
            self.done_urls.append(url)
            yield scrapy.Request(url,callback=self.parse_content,meta={'ROW':row,'REF':REF},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        # Next page
        if len(Data)>0:
            page+=1
            url='https://grailzee.com/apps/auctioneer/api/auctions?q='+quote(SEARCH)+'&page='+str(page)
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True,priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_content(self,response):
        REF=response.meta['REF']
        ROW=response.meta['ROW']
        # BRANDS = [REF['manufacturer']]
        DESC=cleanhtml(response.xpath('//div[@class="product-description"]').get())
        ITEM={}
        ITEM['product_name']=ROW['title']
        Data=response.xpath('//div[@class="metafields"]/span')
        for row in Data:
            TITLE=str(row.xpath('./strong/text()').get()).split(':')[0]
            VAL=str(row.xpath('./text()').get()).strip()
            ITEM[Get_Key_String(str(TITLE).lower())]=VAL
        Data=response.xpath('//div[@class="product-details"]//li/text()').getall()
        for row in Data:
            if ': ' in row:
                rowstr=str(row).split(': ')
                TITLE=str(rowstr[0]).strip()
                VAL=str(rowstr[1]).strip()
                if not Get_Key_String(str(TITLE).lower()) in ITEM:
                    ITEM[Get_Key_String(str(TITLE).lower())]=VAL
        #print(ITEM)

        item={}
        item.update(ITEM_DATA)
        item['listing_uuid']=ROW['id']
        item['brand']=str(ITEM.get('make','')).title()
        item['listing_url']=response.url
        item['ref_number']=ITEM.get('reference_number','')
        if len(Get_Number(ITEM.get('year','')))==4 and Get_Number(ITEM.get('year',''))==ITEM.get('year',''):
            item['production_year']=ITEM.get('year','')
        elif '/' in ITEM.get('year',''):
            YEAR=str(ITEM.get('year','')).split('/')
            for rs in YEAR:
                if Get_Number(rs)==rs and len(Get_Number(rs))==4 and int(Get_Number(rs))<datetime.now().year and item['production year']=='':
                    item['production_year']=rs
        item['dial_color']=ITEM.get('dial','')
        for Type in BRACELET_TYPES:
            if Type in str(ITEM['product_name']).title() and item['bracelet_type']=='':
                item['bracelet_type']=Type
        SIZE=ITEM.get('case_diameter','')
        SIZE=(str(SIZE).lower()).replace(' mm', 'mm').replace('x', ' ')
        for rs in SIZE:
            if not rs in string.ascii_lowercase and not rs.isdigit() and rs!='.':
                SIZE=str(SIZE).replace(rs, ' ')
        SIZE=str(SIZE).split()
        for rs in SIZE:
            if 'mm' in rs and Get_Number(rs)+'mm'==rs and item['case_size']=='':
                item['case_size']=Get_Number(rs)+' mm'
        material=[]
        beze=[]
        for k,v in ITEM.items():
            v=str(v).strip()
            if 'material' in k and not 'beze' in k:
                if not v in material:
                    material.append(v)
            if 'material' in k and 'beze' in k:
                if not v in beze:
                    beze.append(v)
        item['material']='; '.join(material)
        if 'dial_numerals' in ITEM:
            item['numerals']=ITEM.get('dial_numerals')
        if 'dial_numbers' in ITEM:
            item['numerals']=ITEM.get('dial_numbers')
        item['condition']=ITEM.get('condition','')
        if item['condition'] in ['New','Unworn']:
            item['new preowned']='New'
        else:
            item['new preowned']='Preowned'
        item['price']=ROW.get('currentBid','')
        # CURRENT_TIME = (datetime.utcnow()).strftime('%Y-%m-%dT%H:%M:%S')
        END_TIME = dateparser.parse(ROW['ends_at'], settings={'TIMEZONE': '+0300'}).strftime('%Y-%m-%dT%H:%M:%S')

        # if END_TIME < CURRENT_TIME:
        #     item['sale_date'] = END_TIME

        item['model']=ITEM.get('model','')
        item['bezel']='; '.join(beze)
        Box=False
        Papers=False
        if ITEM.get('box','')=='Yes':
            Box=True
        if ITEM.get('papers','')=='Yes':
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
        metadata['search_type'] = 'offer'
        metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
        metadata['event_time']=''
        # if item['sale_date'] != '':
        #     metadata['event_time'] = dateparser.parse(item['sale_date']).strftime('%Y-%m-%d %H:%M:%S')
        #     metadata['search_type'] = 'sale'
        metadata['event_id']=str(item['listing_uuid'])
        metadata['event_url']=item['listing_url']
        scraped_data={}
        scraped_data['price']=item['price']
        scraped_data['title']=ROW['title']
        scraped_data['sub_title']=''
        scraped_data['description']=DESC
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        scraped_data['case_material']=ITEM.get('case_material','')
        scraped_data['bracelet_material']=ITEM.get('bracelet_material','')
        scraped_data['dial_color']=item['dial_color']
        scraped_data['dial_indicies']=item['numerals']
        scraped_data['bezel']=ITEM.get('bezel_material','')
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
        (scraped_data['miscellaneous']).update(ITEM)
        DATASET['metadata']=metadata
        DATASET['scraped_data']=scraped_data

        bid_data_url = f'https://grailzee.com/apps/auctioneer/api/auctions/{item["listing_uuid"]}'
        yield scrapy.Request(bid_data_url,
                             callback=self.parse_sale,
                             priority=10,
                             meta={'DATASET': DATASET, 'END_TIME': END_TIME}, dont_filter=True)

    def parse_sale(self, response):
        END_TIME = response.meta['END_TIME']
        DATASET = response.meta['DATASET']

        # clean / update ends_at
        try:
            DATASET['scraped_data']['miscellaneous']['ends_at'] = END_TIME
            DATASET['scraped_data']['miscellaneous']['endsAt'] = END_TIME
        except Exception:
            pass

        try:
            bid_data = json.loads(response.text)
            if bid_data.get('winningBid') or bid_data.get('hasWinner') or bid_data.get('legacyWinner'):
                DATASET['metadata']['event_time'] = dateparser.parse(END_TIME).strftime('%Y-%m-%d %H:%M:%S')
                DATASET['metadata']['search_type'] = 'sale'
            else:
                bids_url = f'https://grailzee.com/apps/auctioneer/products/{DATASET["metadata"]["event_id"]}/bids'
                yield scrapy.Request(bids_url,
                                     callback=self.parse_bids,
                                     priority=10,
                                     meta={'DATASET': DATASET}, dont_filter=True)
                return
        except Exception as e:
            pass

        yield (DATASET)

    def parse_bids(self, response):
        DATASET = response.meta['DATASET']
        try:
            bids = json.loads(response.text)
            if bids:
                for bid in bids:
                    DATASET['scraped_data']['price'] = bid.get('amount', '')
                    DATASET['scraped_data']['miscellaneous']['currentBid'] = bid.get('amount', '')
                    DATASET['metadata']['event_time'] = dateparser.parse(bid.get('createdAt'), settings={'TIMEZONE': '+0300'}).strftime('%Y-%m-%dT%H:%M:%S')
                    yield DATASET
                return
        except Exception as e:
            pass

        yield(DATASET)
