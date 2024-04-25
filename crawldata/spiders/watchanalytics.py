import scrapy,json,re,os,platform,random,requests
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'watchanalytics'
    url='https://watchanalytics.io'
    DATE_CRAWL=datetime.now()
    custom_settings={'CONCURRENT_REQUESTS':1,'CONCURRENT_REQUESTS_PER_DOMAIN':1,'CONCURRENT_REQUESTS_PER_IP':1,'DOWNLOAD_DELAY':3,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    #custom_settings={'CONCURRENT_REQUESTS':1,'CONCURRENT_REQUESTS_PER_DOMAIN':1,'CONCURRENT_REQUESTS_PER_IP':1,'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    cookies = {'cf_clearance': 'Lm0k7qEld0ZuyXf.iSaS1Ghr.Md21YOPee2.3LbAjsw-1687147458-0-160','currency': 'EUR','language': 'en'}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Alt-Used': 'api.watchanalytics.io','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1',}
    proxies=re.split('\r\n|\n',open('./proxy_25000.txt','r').read())
    def update_cookies(self):
        RUN=True
        PR=[]
        while RUN==True and len(PR)<10:
            proxy = self.proxies[random.randint(0,len(self.proxies)-1)]
            while proxy in PR: 
                proxy = self.proxies[random.randint(0,len(self.proxies)-1)]
            self.logger.info(proxy)
            PR.append(proxy)
            resp = requests.post("http://localhost:8000/challenge",json={"proxy": {"server": proxy}, "timeout": 20,"url": "https://watchanalytics.io/search/Rolex"})
            if resp.json().get("success"):
                self.logger.info('challenge success')
                ua = resp.json().get("user_agent")
                cf_clearance_value = resp.json().get("cookies").get("cf_clearance")
                # use cf_clearance, must be same IP and UA
                headers = {"User-Agent": ua}
                cookies = {"cf_clearance": cf_clearance_value}
                res = requests.get('https://api.watchanalytics.io/v1/products/?type=search&query=Rolex', proxies={"all": proxy}, headers=headers, cookies=cookies)
                if '<title>Just a moment...</title>' not in res.text:
                    self.logger.info('watchanalytics test request succeeded')
                    self.headers.update(headers)
                    self.cookies.update(cookies)
                    self.proxy=proxy
                    RUN=False
    def start_requests(self):
        self.update_cookies()
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
                SEARCH=str(' '.join(KEY)).lower()
                page=1
                url='https://api.watchanalytics.io/v1/products/?type=search&query='+quote(SEARCH)+'&page='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page,'proxy':'http://'+self.proxy},dont_filter=True,cookies=self.cookies,headers=self.headers)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        try:
            Data=json.loads(response.text)
            for row in Data['products']:
                file_url = 'https://watchanalytics.io/products/' + row['slug']
                if file_url in self.done_urls:
                    self.logger.info(f'URL SCRAPPED BEFORE: {file_url}')
                    continue
                update_scrapped_urls(self.name, file_url, self.logger)
                self.done_urls.append(file_url)

                url='https://api.watchanalytics.io/v1/products/'+row['slug']+'/'
                yield scrapy.Request(url,callback=self.parse_content,meta={'REF':REF,'row':row,'proxy':'http://'+self.proxy},dont_filter=True,cookies=self.cookies,headers=self.headers)
            if page<Data['pages']:
                page+=1
                url='https://api.watchanalytics.io/v1/products/?type=search&query='+quote(SEARCH)+'&page='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page,'proxy':'http://'+self.proxy},dont_filter=True,cookies=self.cookies,headers=self.headers)
        except:
            self.update_cookies()
            yield scrapy.Request(response.url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page,'proxy':'http://'+self.proxy},dont_filter=True,cookies=self.cookies,headers=self.headers)
    def parse_content(self,response):
        REF=response.meta['REF']
        BRANDS=(str(REF).split('~')[0]).split('|')
        row=response.meta['row']
        try:
            Data=json.loads(response.text)
            IT=Data['details']
            item={}
            item.update(ITEM_DATA)
            item['listing_uuid']=row['slug']
            item['brand']=IT['Brand']
            item['listing_url']='https://watchanalytics.io/products/'+row['slug']
            item['ref_number']=Data['ref']
            item['production_year']=''
            item['dial_color']=IT['Dial']
            item['bracelet_type']=''
            item['case_size']=IT['Diameter']+'mm'
            item['material']=IT['Material']
            item['price']=row['price']
            item['bezel']=IT.get('bezel','')
            item['model']=IT.get('model','')
            TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
            for Type in TYPES:
                if Type in Data['name'] and item['bracelet_type']=='':
                    item['bracelet_type']=Type
            LAB=list(Data['prices'].keys())
            VAL=list(Data['prices'].values())
            for i in range(1,13):
                item['sale_date '+str(i)]=''
                item['history_price '+str(i)]=''
            k=1
            i=12
            while i>0:
                if k<len(LAB):
                    item['sale_date '+str(i)]=LAB[len(LAB)-k]
                    item['history_price '+str(i)]=VAL[len(VAL)-k]
                    k+=1
                i-=1
            item['brand']=str(item['brand']).title()
            #Scraped data
            DATASET={}
            metadata={}
            metadata['marketplace']=str(self.name).title()
            metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
            metadata['search_type']='oracle'
            metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
            metadata['event_time']=''
            metadata['event_id']=str(item['listing_uuid'])
            metadata['event_url']=item['listing_url']
            DATASET['metadata']=metadata
            scraped_data={}
            scraped_data['price']=item['price']
            scraped_data['title']=Data['name']
            scraped_data['sub_title']=''
            scraped_data['description']=''
            for rs in Data['panels']:
                if 'description' in rs:
                    scraped_data['description']=rs['description']
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
            (scraped_data['miscellaneous']).update(row)
            (scraped_data['miscellaneous']).update(Data)
            DATASET['scraped_data']=scraped_data
            yield(DATASET)
        except:
            self.update_cookies()
            yield scrapy.Request(response.url,callback=self.parse_content,meta={'REF':REF,'row':row,'proxy':'http://'+self.proxy},dont_filter=True,cookies=self.cookies,headers=self.headers)
