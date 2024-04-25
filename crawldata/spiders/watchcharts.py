import time

import scrapy,json,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'watchcharts'
    url='https://watchcharts.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    #custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Alt-Used': 'watchcharts.com','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1'}
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
                Page=1
                url='https://watchcharts.com/listings?q='+SEARCH+'&source=forums&status=sold&page='+str(Page)
                yield scrapy.Request(self.URL,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'Page':Page,'URL':url},dont_filter=True,headers=self.headers, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self,response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        Page=response.meta['Page']
        URL=response.meta['URL']
        RES = requests.get(URL,headers=self.headers)
        response=scrapy.Selector(text=RES.text)
        Data=response.xpath('//div[@id="pagination-page"]/div/div')
        for row in Data:
            PRICE=row.xpath('.//h4/text()').get()
            url=self.url+row.xpath('.//a[contains(@href,"/listing/")]/@href').get()
            if url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {url}')
                continue
            update_scrapped_urls(self.name, url, self.logger)
            self.done_urls.append(url)
            yield scrapy.Request(self.URL,callback=self.parse_content,meta={'REF':REF,'PRICE':PRICE,'URL':url},headers=self.headers,dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        if len(Data)>=30:
            Page+=1
            url='https://watchcharts.com/listings?q='+SEARCH+'&source=forums&status=sold&page='+str(Page)
            yield scrapy.Request(self.URL,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'Page':Page,'URL':url},dont_filter=True,headers=self.headers, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_content(self,response):
        REF=response.meta['REF']
        ITEM={}
        ITEM['PRICE']=response.meta['PRICE']
        URL=response.meta['URL']
        RES = requests.get(URL,headers=self.headers)
        response=scrapy.Selector(text=RES.text)
        ITEM['NAME']=cleanhtml(response.xpath('//div[contains(@class,"mb-2")]//h5').get())
        ITEM['DATE']=response.xpath('//span[@data-toggle="tooltip"]/time[@datetime]/@datetime').get()
        ITEM['BRAND']=''
        ITEM['MODEL']=''
        ITEM['REF']=''
        BRAND=response.xpath('//a[@data-target="#brandTab"]/text()').get()
        if BRAND:
            ITEM['BRAND']=str(BRAND).strip()
        Data=response.xpath('//nav[@aria-label="breadcrumb"]//li/a')
        for row in Data:
            link=row.xpath('./@href').get()
            txt=row.xpath('./text()').get()
            if '/brand/' in link and str(link).count('/')==3 and ITEM['BRAND']=='':
                ITEM['BRAND']=txt
            if '/brand/' in link and str(link).count('/')==4:
                ITEM['MODEL']=txt
            if '/watch_model/' in link:
                ITEM['MODEL']+=' '+txt
                ITEM['MODEL']=str(ITEM['MODEL']).strip()
                ITEM['REF']=txt
        ID=str(URL).split('/')
        ID=ID[len(ID)-1]
        url=self.url+'/listings/description/'+ID
        yield scrapy.Request(self.URL,callback=self.parse_content_html,meta={'REF':REF,'ITEM':ITEM,'ID':ID,'P_URL':URL,'URL':url},headers=self.headers,dont_filter=True, priority=10)
    def parse_content_html(self,response):
        REF=response.meta['REF']
        BRANDS=(str(REF).split('~')[0]).split('|')
        ID=response.meta['ID']
        ITEM=response.meta['ITEM']
        NAME=ITEM['NAME']
        URL=response.meta['URL']
        P_URL=response.meta['P_URL']
        RES = requests.get(URL,headers=self.headers)
        response=scrapy.Selector(text=RES.text)
        DESC=response.xpath('//div[@class="listing-description-text"]//text()').getall()
        DESCS=[]
        IT={}
        for TXT in DESC:
            TXT=str(TXT).strip()
            if TXT!='':
                DESCS.append(TXT)
                if ':' in TXT:
                    TXT=TXT.split(':')
                    TITLE=Get_Key_Str(TXT[0]).lower()
                    IT[TITLE]=''
                    del TXT[0]
                    IT[TITLE]=' '.join(TXT)
        DESC=' '.join(DESCS)
        item={}
        item.update(ITEM_DATA)
        item['listing_uuid']=ID
        item['brand']=ITEM['BRAND']
        item['listing_url']=P_URL
        item['ref_number']=ITEM['REF']
        item['new_preowned']='Preowned'
        item['complete_set']='FALSE'
        item['price']=ITEM['PRICE']
        item['sale_date']=ITEM['DATE']
        item['bezel']=''
        item['model']=ITEM['MODEL']
        for k,v in IT.items():
            if k in item and k!='price':
                item[k]=v
            if 'year' in str(k).lower() and len(Get_Number(v))==4:
                item['production_year']=Get_Number(v)
        if item['condition']=='' and 'conditions' in IT:
            item['condition']=IT['conditions']
        if len(Get_Number(item['condition']))==4 and item['production_year']=='' and not '/' in item['condition']:
            item['production_year']=Get_Number(item['condition'])
        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
        for Type in TYPES:
            if str(Type).lower() in str(NAME).lower() and item['bracelet_type']=='':
                item['bracelet_type']=Type
        for Type in TYPES:
            if str(Type).lower() in str(DESC).lower() and item['bracelet_type']=='':
                item['bracelet_type']=Type
        if 'unworn' in str(DESC+' '+NAME).lower() or 'NEW' in (DESC+' '+NAME) or 'new condition' in (DESC+' '+NAME):
            item['new_preowned']='New'
        if 'like new' in str(DESC+' '+NAME).lower():
            item['new_preowned']='Preowned'
        Box=False
        Papers=False
        if 'full set' in str(DESC+' '+NAME).lower() or 'complete set' in str(DESC+' '+NAME).lower() or 'full-set' in URL or 'complete-set' in URL:
            Box=True
            Papers=True
        if 'box' in str(NAME).lower() or 'box' in str(DESC).lower():
            Box=True
        if 'papers' in str(NAME).lower() or 'papers' in str(DESC).lower():
            Papers=True
        if Box==True and Papers==True:
            item['complete_set']='TRUE'
        elif Box==True:
            item['complete_set']='box'
        elif Papers==True:
            item['complete_set']='papers'
        for color in COLORS:
            if color in str(NAME).lower() and item['dial_color']=='':
                item['dial_color']=str(color).title()
        item['dial_color']=str(item['dial_color']).strip()
        STR_NAME=str(NAME).split()
        for rs in STR_NAME:
            if len(Get_Number(rs))==4 and len(Get_Year(rs))==len(str(rs).strip()) and item['production_year']=='' and int(Get_Number(rs))<=datetime.now().year and int(Get_Number(rs))>1900:
                item['production_year']=Get_Number(rs)
        if item['production_year']=='':
            YEAR=''
            for rcs in STR_NAME:
                if len(Get_Number(rcs))>=4:
                    if '/' in rcs:
                        STR=str(rcs).split('/')
                        for rs in STR:
                            if len(Get_Number_Only(rs))==4 and Get_Number_Only(rs)==rs and int(rs)<=datetime.now().year and YEAR=='' and int(rs)>1900:
                                YEAR=rs
                    elif len(Get_Number(rcs))==4 and Get_Number_Only(rs)==rs and int(Get_Number(rcs))<datetime.now().year and len(Get_Year(rcs))==len(str(rcs).strip()) and YEAR=='' and int(Get_Number(rcs))>1900:
                        YEAR=rcs
            if YEAR!='':
                item['production_year']=YEAR
        if item['brand']=='':
            for BR in BRANDS:
                if BR in str(NAME).upper() and item['brand']=='':
                    item['brand']=str(BR).title()
        if 'mm' in DESC:
            TXT=str(DESC).split()
            for Txt in TXT:
                if 'mm' in Txt and len(Get_Number_Only(Txt))>1 and item['case_size']=='':
                    item['case_size']=Txt
        if 'mm' in URL+' '+NAME +' '+DESC:
            TXT=re.split('-| ', str(URL+' '+NAME +' '+DESC))
            for Txt in TXT:
                if 'mm' in Txt and len(Get_Number_Only(Txt))>1 and item['case_size']=='':
                    item['case_size']=Txt
        item['brand']=str(item['brand']).title()
        #Scraped data
        DATASET={}
        metadata={}
        metadata['marketplace']=str(self.name).title()
        metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
        metadata['search_type']='oracle'
        metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
        metadata['event_time']=item['sale_date']
        if metadata['event_time']!='':
            metadata['search_type']='sale'
        metadata['event_id']=str(item['listing_uuid'])
        metadata['event_url']=item['listing_url']
        DATASET['metadata']=metadata
        scraped_data={}
        scraped_data['price']=item['price']
        scraped_data['title']=NAME
        scraped_data['sub_title']=''
        scraped_data['description']=DESC
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=IT.get('bracelet_material','')
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
        (scraped_data['miscellaneous']).update(IT)
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
