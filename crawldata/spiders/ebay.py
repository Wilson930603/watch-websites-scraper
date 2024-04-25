import time

from scrapy.selector import Selector
import scrapy,json,dateparser,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'ebay'
    domain='https://www.ebay.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    # custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    # cookies = {'dp1': 'bpbf/^%^230000000000c000e0000001000200000065f4f9e5^^u1p/QEBfX0BAX19AQA**67d62d65^^bl/US67d62d65^^','nonsession': 'BAQAAAYbdaqBpAAaAAMoAIGfWLWUwN2U3MzkyNjE4NjBhNDRkNTQxM2I2OGRmZmY2ZDUyNAAzAAll9PnlMDcwOTQsVVNBAMsAAmQTzW0yNkj0NcMZbM1UURUI7XrVe5+Ixmrr','__deba': 'aXYb059IDhuUQaIuxi5DYwlimoU86nt54vPK7t0M_syvkH_q9VOzkH1xprQEz4XYhZcfmgAixzFAncBdioUhpyPkFMTjHUSCj_SzzOqCp-Oap71ZQpdoxhVd1hEgxCAxQZpA20Earg7UTiugi7Fx7g==','__uzma': '71ce7bb4-e9ef-4531-8932-6040df2db191','__uzmb': '1675169847','__uzmc': '4181423869362','__uzmd': '1678776803','__uzme': '0262','__uzmf': '7f60009b1a55b6-da8f-4c5d-ab95-61291a926c9d16751698475583606955593-730feb329f7e29ad238','__ssds': '2','__ssuzjsr2': 'a9be0cd8e','__gads': 'ID=aae0c43ef61d01bd:T=1675169853:S=ALNI_MaiIeXVMIKVCZ1a7LH_q4wsDoIyeg','__gpi': 'UID=000009995392df39:T=1675169853:RT=1679017489:S=ALNI_Ma85TSn2st4bssowb1k_Ii6Phd7cA','cid': 'JClx981TAsARBMgS^%^232065445537','ns1': 'BAQAAAYbdaqBpAAaAANgATGX0+eVjNzJ8NjAxXjE2NzUxNjk5ODcwMDheXjFeM3wyfDV8NHw3fDExXjFeMl40XjNeMTJeMTJeMl4xXjFeMF4xXjBeMV42NDQyNDU5MDc1fJD4LRjwK3KgcH6IM0sKKQ8qz9w*','__gsas': 'ID=dd18647d7b9c2945:T=1675400604:S=ALNI_MZ2V5gthQE93UUoNT8VM5yFqjqXjQ','QuantumMetricUserID': '218458ced06aa6ab0c96f5796f1fcb9f','s': 'CgAD4ACBkFRflMDdlNzM5MjYxODYwYTQ0ZDU0MTNiNjhkZmZmNmQ1MjQA7gByZBUX5TMGaHR0cHM6Ly93d3cuZWJheS5jb20vc2NoL2kuaHRtbD9fZnJvbT1SNDAmX3Rya3NpZD1wMjM4MDA1Ny5tNTcwLmwxMzEyJl9ua3c9Um9sZXgrMTI0MzAwJl9zYWNhdD0wI2l0ZW0xZjM5M2RkNmMxB8CDfaY*','ebay': '^%^5Ejs^%^3D1^%^5Esbf^%^3D^%^2310000100000^%^5E','ak_bmsc': '983EB92E810391C7FA4C70E7441B585A~000000000000000000000000000000~YAAQtwyrcdrO1OOGAQAAwaI97ROiC6gZqDV3tB3P+NpyMwzIy3iwgRHoBa6VVazUfREzU0NVHAJS1nmWOugNS5EykSa+BK1hGwlv0ixuNGGZxzWhLZ1+8RB1VKXuE86yL/QATUkAAKvXMQKvyjEJgFEhm3bBO2QDqZX72ev0xIMD+/JhaZjxbcDcZtiKShRFlFqpKMFjycDDRs6miFxpC1gJHIAac4wZppnZN9H8ddR653zve8jjeuI1uDpSUx9fdqUIfC70ajuYHbcRLBawNccQhY1g9eVoMmnISBEJ5Sb3LQUO4PVTMBOM76R59+XIbqJ/4HmhOSwIbKgtIknRPFBe/TAXV7K4ANTnSPWWG/l52unXJPanm6njr+t4mRRKf+k7Q9D/YexO','bm_sv': '8F19E9E5C71FF4C3F771EF5E8C18753A~YAAQMywtF9N59smGAQAAufs+7RNfv5EMHeHlKgs7+5qXlxa9Y+TRTZy9W0V3/MezCwr/ugnYOr1AQsVdomzlOzx/b+98Fa08MPs59HzLXtj41DH3tcqGTijlOtRN4C4Ky0qK/5NlNiJVXUqXHO8rkwS5lZpEfVuozkZZkCskxDdjhtNPf1rnemr4Gpyw1gfdBohSrW6CeK+WxzJ7Z8MFmUdGB5AtA355GJR2dXfXlt/X0Kean21tndbmP4WmP2k=~1','ds2': 'sotr/b7pwxzzzzzzz^^','JSESSIONID': '921B5215954852CD17B8F96B8529A858'}
    IDS=[]
    def start_requests(self):
        self.done_urls = get_scrapped_urls(self.name, self.logger)

        self.done_urls = [url.split('?')[0] for url in self.done_urls]

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
                url='https://www.ebay.com/sch/i.html?_from=R40&_trksid=p2334524.m570.l1313&_nkw='+SEARCH+'&_sacat=260325&LH_TitleDesc=0&LH_Sold=1'
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'page':1,'Level':0},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        # check proper crawlera response
        if 'User account suspended' in response.text:
            log_error_message(self.sns_client, CONFIGS['SNS_Topic_arn'], self.logger, self.name.title(),
                              response.url,
                              'CRITICAL', 'RATE_LIMIT_REACHED',
                              f'Proxy API rate limit reached, spider {self.name} will close')
            self.crawler.engine.close_spider(self, 'API was suspended')
            return

        REF=response.meta['REF']
        page=response.meta['page']
        Level=response.meta['Level']
        if response.status==200:
            response = Selector(text=response.text)
            Data=response.xpath('//ul[contains(@class,"srp-results")]/li[@data-viewport]')
            for row in Data:
                url=row.xpath('.//a/@href').get()
                file_url = url.split('?')[0]
                if file_url in self.done_urls:
                    self.logger.info(f'URL SCRAPPED BEFORE: {file_url}')
                    continue
                update_scrapped_urls(self.name, file_url, self.logger)
                self.done_urls.append(file_url)

                if '/itm/' in url:
                    ID=str(url).split('?')[0]
                    ID=str(ID).split('/')
                    ID=str(REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']).strip()+'_'+ID[len(ID)-1]
                    if not ID in self.IDS:
                        self.IDS.append(ID)
                        STRDATE=row.xpath('.//span[@class="POSITIVE" and(contains(text(),"Sold"))]/text()').get()
                        STRDATE=str(STRDATE.replace('Sold','')).strip()
                        Date=''
                        if len(STRDATE)>10:
                            Date=dateparser.parse(STRDATE).strftime('%Y-%m-%d')
                        ITEM={}
                        ITEM['Date']=Date
                        ITEM['type of sale']=''
                        TYPE=row.xpath('.//span[@class="s-item__purchase-options s-item__purchaseOptions"]/text()').get()
                        if TYPE:
                            ITEM['type of sale']=str(TYPE).replace('or Best Offer', 'Best Offer')
                        NEW=row.xpath('.//div[@class="s-item__subtitle"]/span[@class="SECONDARY_INFO"]/text()').get()
                        ITEM['new preowned']=''
                        if 'Pre-Owned' in str(NEW):
                            ITEM['new preowned']='Preowned'
                        elif 'New' in str(NEW):
                            ITEM['new preowned']='New'
                        ITEM['price']=''
                        PRICE=row.xpath('.//span[@class="s-item__price"]//span[@class="POSITIVE"]/text()').get()
                        if PRICE:
                            ITEM['price']=PRICE
                            LISTED=row.xpath('.//span[@class="s-item__trending-price"]//span[@class="STRIKETHROUGH"]/text()').get()
                            ITEM['listed_price']=''
                            if LISTED:
                                ITEM['listed_price']=LISTED
                            yield scrapy.Request(url,callback=self.parse_content,meta={'REF':REF,'Level':0,'ITEM':ITEM},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
                    else:
                        f=open(LOG_PATH+'ebay_duplicate.txt','a',encoding='utf-8')
                        f.write(ID+'\n')
                        f.close()
            next_page=response.xpath('//a[@class="pagination__next icon-link"]/@href').get()
            if next_page:
                page+=1
                yield scrapy.Request(next_page,callback=self.parse,meta={'REF':REF,'page':page,'Level':0},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
        elif Level<50:
            Level+=1
            yield scrapy.Request(response.url,callback=self.parse,meta={'REF':REF,'page':page,'Level':Level},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
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
        Level=response.meta['Level']
        ITEM_OLD=response.meta['ITEM']
        ID=str(response.url).split('?')[0]
        ID=str(ID).split('/')
        ID=ID[len(ID)-1]
        if response.status==200:
            Link=response.xpath('//div[@class="app-cvip-message-container"]//a/@href').get()
            if Link and Link not in self.done_urls:
                update_scrapped_urls(self.name, Link, self.logger)
                self.done_urls.append(Link)
                yield scrapy.Request(Link,callback=self.parse_content,meta={'REF':REF,'Level':0,'ITEM':ITEM_OLD},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
            else:
                Link=response.xpath('//span[contains(@class,"original-listing")]/a/@href').get()
                if Link and Link not in self.done_urls:
                    update_scrapped_urls(self.name, Link, self.logger)
                    self.done_urls.append(Link)
                    yield scrapy.Request(Link,callback=self.parse_content,meta={'REF':REF,'Level':0,'ITEM':ITEM_OLD},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
            if Link is None:
                item={}
                item.update(ITEM_DATA)
                item['listing_uuid']=ID
                item['listing_url']=response.url
                item['new_preowned']=ITEM_OLD['new preowned']
                item['complete_set']='FALSE'
                item['listed_price']=ITEM_OLD['listed_price']
                item['price']=ITEM_OLD['price']
                item['sale_date']=ITEM_OLD['Date']
                item['type_of_sale']=ITEM_OLD['type of sale']
                NAME=cleanhtml(response.xpath('//h1').get())
                Data=response.xpath('//div[contains(@class,"ux-layout-section")]')
                ITEM={}
                for rows in Data:
                    LABELS=rows.xpath('./div[@class="ux-labels-values__labels"]').getall()
                    VALUES=rows.xpath('./div[@class="ux-labels-values__values"]').getall()
                    for i in range(len(LABELS)):
                        LABEL=str(cleanhtml(LABELS[i])).replace(':', '')
                        VALUE=cleanhtml(VALUES[i])
                        ITEM[((str(LABEL).strip()).lower()).replace(' ', '_')]=str(VALUE).strip()
                Data=response.xpath('//section[@class="product-spectification"]//li')
                for rows in Data:
                    LABEL=rows.xpath('./div[@class="s-name"]/text()').get()
                    VALUE=rows.xpath('./div[@class="s-value"]/text()').get()
                    ITEM[((str(LABEL).strip()).lower()).replace(' ', '_')]=str(VALUE).strip()
                for k,v in item.items():
                    if v=='' and k in ITEM:
                        item[k]=ITEM[k]
                        if '12-Stunden' in ITEM[k] and item['numerals']=='':
                            item['numerals']=ITEM[k]
                if 'case_material' in ITEM:
                    item['material']=ITEM['case_material']
                if item['dial_color']=='' and 'color' in ITEM:
                    item['dial_color']=ITEM['color']
                if item['dial_color']=='' and 'dial_colour' in ITEM:
                    item['dial_color']=ITEM['dial_colour']
                TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
                if item['bracelet_type']=='':
                    if 'band/strap' in ITEM:
                        for Type in TYPES:
                            if Type in ITEM['band/strap'] and item['bracelet_type']=='':
                                item['bracelet_type']=Type
                if item['bracelet_type']=='':
                    for Type in TYPES:
                        if Type in NAME and item['bracelet_type']=='':
                            item['bracelet_type']=Type
                TXT=str(item['condition']).split(':')[0]
                if item['new_preowned']=='':
                    if 'New' in TXT:
                        item['new_preowned']='New'
                    elif 'Pre-' in TXT or 'owned' in TXT:
                        item['new_preowned']='Preowned'
                TXT=cleanhtml(response.xpath('//h1').get())
                TXT=TXT+' '+cleanhtml(response.xpath('//div[@data-testid="d-item-description"]').get())
                TXT=str(TXT).lower()
                Box=False
                Papers=False
                if 'with original box/packaging' in ITEM:
                    if ITEM['with original box/packaging']=='Yes':
                        Box=True
                if 'with papers' in ITEM:
                    if ITEM['with papers']=='Yes':
                        Papers=True
                if Box==True and Papers==True:
                    item['complete_set']='TRUE'
                elif 'full set' in TXT or 'complete set' in TXT:
                    item['complete_set']='TRUE'
                elif Box==True:
                    item['complete_set']='box'
                elif Papers==True:
                    item['complete_set']='papers'
                    
                TYPES=response.xpath('//div[@data-testid="x-label"]/span/text()').getall()
                TYPES2=response.xpath('//div[@data-testid="x-bin-price"]//span[@data-testid="ux-textual-display"]//text()').getall()
                for i in range(len(TYPES)):
                    TYPES[i]=(str(TYPES[i]).replace(':', '')).lower()
                TYPE=['buy it now','auction','best offer','Best offer accepted']
                if item['type_of_sale']=='':
                    for Type in TYPE:
                        if Type in TYPES and item['type_of_sale']=='':
                            item['type_of_sale']=Type
                if item['type_of_sale']=='':
                    for Type in TYPE:
                        if Type in TYPES2 and item['type_of_sale']=='':
                            item['type_of_sale']=Type
                item['ref_number']=ITEM.get('reference_number','')
                item['production_year']=ITEM.get('year_manufactured','')
                if '-' in item['production_year']:
                    item['production_year']=(str(item['production_year']).split('-')[0]).strip()
                for color in COLORS:
                    if color in str(NAME).lower() and item['dial_color']=='':
                        item['dial_color']=str(color).title()
                if item['brand']=='':
                    for BR in BRANDS:
                        if BR in str(NAME).upper() and item['brand']=='':
                            item['brand']=str(BR).title()
                item['model']=str(ITEM.get('model','')).strip()
                item['bezel']=ITEM.get('bezel_type','')
                item['brand']=str(item['brand']).title()
                item['case_size']=ITEM.get('case_size','')
                #Scraped data
                DATASET={}
                metadata={}
                metadata['marketplace']=str(self.name).title()
                metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
                metadata['search_type']='sale'
                metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
                metadata['event_time']=''
                if item['sale_date']!='':
                    metadata['event_time']=dateparser.parse(item['sale_date']).strftime('%Y-%m-%d %H:%M:%S')
                metadata['event_id']=str(item['listing_uuid'])
                metadata['event_url']=str(response.url).replace('.json', '')
                scraped_data={}
                scraped_data['price']=item['price']
                scraped_data['title']=NAME
                scraped_data['sub_title']=''
                scraped_data['description']=''
                scraped_data['manufacturer']=item['brand']
                scraped_data['model']=item['model']
                scraped_data['reference_number']=item['ref_number']
                scraped_data['case_material']=item.get('material','')
                scraped_data['bracelet_material']=ITEM.get('bracelet','')
                scraped_data['dial_color']=item['dial_color']
                scraped_data['dial_indicies']=ITEM.get('indices','')
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
                (scraped_data['miscellaneous']).update(ITEM_OLD)
                (scraped_data['miscellaneous']).update(ITEM)
                DATASET['metadata']=metadata
                DATASET['scraped_data']=scraped_data
                url=response.xpath('//iframe[@id="desc_ifr"]/@src').get()
                if url:
                    yield scrapy.Request(url,callback=self.parse_body,meta={'DATASET':DATASET},dont_filter=True, priority=10)
                else:
                    yield(DATASET)
        elif Level<5:
            Level+=1
            yield scrapy.Request(response.url,callback=self.parse_content,meta={'REF':REF,'Level':Level,'ITEM':ITEM_OLD},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
    def parse_body(self,response):
        DATASET=response.meta['DATASET']
        DATASET['scraped_data']['description']=cleanhtml(response.xpath('//body').get())
        yield(DATASET)
