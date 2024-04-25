import time
import scrapy,json,re,os,platform,requests
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3
from twocaptcha import TwoCaptcha


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'valueyourwatch'
    url='https://valueyourwatch.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    # custom_settings={'DOWNLOAD_DELAY':1.5, 'CONCURRENT_REQUESTS':2,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Referer': 'https://valueyourwatch.com/','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'same-origin','Sec-Fetch-User': '?1'}
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

            # # Pass captcha if meet
            # api_key = "d368d93ac4830dc4a52b5351a7b92626"
            # url='https://valueyourwatch.com/?s=Rolex%2016233%20&post_type=product&product_cat='
            # response=requests.get(url,headers=self.headers)
            # if "'sitekey': '" in response.text:
            #     self.logger.info('Pass captcha')
            #     sitekey=str(response.text).split("'sitekey': '")[1].split("'")[0]
            #     self.logger.info(f'sitekey:{sitekey}')
            #     solver = TwoCaptcha(api_key)
            #     result = solver.recaptcha(sitekey=sitekey, url=response.url)
            #     data = {'g-recaptcha-response': str(result['code']),'submit': 'Submit'}
            #     response = requests.post('https://valueyourwatch.com/.lsrecap/recaptcha?s=Rolex%2016233%20&post_type=product&product_cat=', data=data,headers=self.headers)
            #     time.sleep(3)
            #     response=requests.get(url,headers=self.headers)
            #     if "'sitekey': '" in response.text:
            #         self.logger.info("Pass captcha UnSuccess !!!")
            #     else:
            #         self.logger.info("Pass captcha Success !!!")
            # # END

            for REF in brand_data:
                KEY=[REF['manufacturer'],REF['model_name'],REF['model_number']]
                SEARCH=' '.join(KEY)
                page=0
                url='https://valueyourwatch.com/?s='+SEARCH+'&post_type=product&product_cat='
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF},dont_filter=True,headers=self.headers)
    def parse(self, response):
        REF=response.meta['REF']
        Data=response.xpath('//figure')
        for row in Data:
            item={}
            item['brand']=str(row.xpath('..//a[@class="woocat"]/text()').get()).replace(' Watches', '')
            item['name']=str(row.xpath('../h3/a/text()').get()).strip()
            item['link']=row.xpath('../h3/a/@href').get()
            if item['link'] and item['link'] not in self.done_urls:
                update_scrapped_urls(self.name, item['link'], self.logger)
                self.done_urls.append(item['link'])
                yield scrapy.Request(item['link'],callback=self.parse_content,meta={'REF':REF,'ITEM':item})
            else:
                self.logger.info(f'URL SCRAPPED BEFORE: {item["link"]}')

    def parse_content(self,response):
        REF=response.meta['REF']
        ITEM=response.meta['ITEM']
        BRANDS=[REF['manufacturer']]
        ID=str(response.xpath('//div[contains(@id,"product-")]/@id').get()).replace('product-', '')
        Data=response.xpath('//div[@id="section-description"]//li').getall() or response.xpath('//div[@class="ux-labels-values__labels"]').getall()
        IT={}
        for TXT in Data:
            TXT=cleanhtml(TXT)
            STR=str(TXT).split(':')
            if len(STR)>1:
                IT[str(STR[0]).lower()]=str(STR[1]).strip()
        DATA=response.xpath('//div[@id="section-description" or @class="woocommerce-product-details__short-description"]//p').getall()
        for TXTS in DATA:
            if str(TXTS).count(':')>3 and str(TXTS).count('<br>')>3:
                Data=str(TXTS).split('<br>')
                for TXT in Data:
                    TXT=cleanhtml(TXT)
                    STR=str(TXT).split(':')
                    if len(STR)>1 and not ((str(STR[0]).lower()).replace('– ', '')).strip() in IT:
                        IT[((str(STR[0]).lower()).replace('– ', '')).strip()]=str(STR[1]).strip()
        DATA=response.xpath('//div[@id="section-description"]/div/div').getall()
        for TXTS in DATA:
            TXTS=str(TXTS).replace('</strong>', ':')
            if str(TXTS).count(':')>3 and str(TXTS).count('<br>')>3:
                Data=str(TXTS).split('<br>')
                for TXT in Data:
                    TXT=cleanhtml(TXT)
                    STR=str(TXT).split(':')
                    if len(STR)>1 and not (str(STR[0]).lower()).replace('– ', '') in IT:
                        IT[(str(STR[0]).lower()).replace('– ', '')]=str(STR[1]).strip()
        DATA=response.xpath('//div[@class="woocommerce-product-details__short-description"]//p').getall()
        for TXTS in DATA:
            if str(TXTS).count('<br>')>3:
                Data=str(TXTS).split('<br>')
                if 'Details' in Data[0]:
                    del Data[0]
                i=0
                while i<len(Data)-1:
                    if not ((str(Data[i]).lower())).strip() in IT:
                        IT[((str(Data[i]).lower())).strip()]=str(Data[i+1]).strip()
                    i+=2
        for TXTS in DATA:
            if str(TXTS).count(':')>0:
                Data=str(TXTS).split(':')
                TITLE=((str(Data[0]).lower())).strip()
                VALUE=str(Data[1]).strip()
                if len(TITLE)<50 and len(VALUE)<100 and not TITLE in IT:
                    IT[TITLE]=VALUE
        DATA=response.xpath('//div[@id="section-description" or @class="woocommerce-product-details__short-description"]//p//text()').getall()
        bezel=''
        for TXTS in DATA:
            if ':' in TXTS:
                STR=str(TXTS).split(':')
                if len(STR)>1 and not ((str(STR[0]).lower())).strip() in IT:
                    TITLE=((str(STR[0]).lower())).strip()
                    del STR[0]
                    VAL=' , '.join(STR)
                    IT[TITLE]=str(VAL).strip()
                if len(STR)>1 and (str(STR[0]).lower()).strip()=='bezel':
                    bezel=(str(STR[1]).lower()).strip().replace('</p>','')
            if '–' in TXTS:
                STR=str(TXTS).split('–')
                if len(STR)>1 and not ((str(STR[0]).lower())).strip() in IT:
                    TITLE=((str(STR[0]).lower())).strip()
                    del STR[0]
                    VAL=' , '.join(STR)
                    IT[TITLE]=str(VAL).strip()
        DESC=cleanhtml('<br>'.join(response.xpath('//div[contains(@class,"description")]').getall()))
        item={}
        item.update(ITEM_DATA)
        item['listing_uuid']=ID
        item['brand']=ITEM['brand']
        item['listing_url']=response.url
        for k,v in IT.items():
            if 'reference_number' in (str(k).replace(' ', '_')).lower():
                item['ref_number']=v
        if item['ref_number']=='':
            for k,v in IT.items():
                if ((str(k).replace(' ', '_')).lower()).endswith('reference'):
                    item['ref_number']=v
        item['complete_set']='FALSE'
        item['bezel']=bezel
        try:
            item['price']=str(Get_Number(response.xpath('//p[@class="price"]//ins//bdi/text()').get())).split('.')[0]
        except:
            item['price']=str(Get_Number(response.xpath('//p[@class="price"]//bdi/text()').get())).split('.')[0]
        if item['price']=='':
            item['price']=str(Get_Number(response.xpath('//p[@class="price"]//bdi/text()').get())).split('.')[0]
        NAME=str(response.xpath('//h1/text()').get()).strip()
        try:
            item['dial_color']=IT['dial color']
        except:
            pass
        TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
        for Type in TYPES:
            if Type in NAME and item['bracelet_type']=='':
                item['bracelet_type']=Type
        try:
            item['production_year']=IT['year manufactured']
        except:
            try:
                item['production_year']=IT['year']
            except:
                pass
        try:
            item['condition']=IT['condition']
        except:
            pass
        if 'UNWORN' in item['condition'] or 'New' in item['condition']:
            item['new_preowned']='New'
        else:
            item['new_preowned']='Preowned'
        Box=False
        Papers=False
        if 'boxes' in response.url:
            Box=True
        if 'boxes' in str(NAME+' '+str(DATA)).lower() or 'box' in str(NAME+' '+str(DATA)).lower():
            Box=True
        if 'papers' in response.url:
            Papers=True
        if 'papers' in str(NAME+' '+str(DATA)).lower():
            Papers=True
        for k,v in IT.items():
            if 'papers' in str(v).lower():
                Papers=True
            if 'boxes' in str(v).lower() or 'box' in str(v).lower():
                Box=True
            if 'mm' in v:
                TXT=str(v).split()
                for i in range(len(TXT)):
                    if 'mm' in TXT[i] and len(Get_Number(TXT[i-1]+TXT[i]))>1 and item['case_size']=='':
                        item['case_size']=Get_Number(TXT[i-1]+TXT[i])
        if Box==True and Papers==True:
            item['complete_set']='TRUE'
        elif Box==True:
            item['complete_set']='box'
        elif Papers==True:
            item['complete_set']='papers'
        #BRAND=response.xpath('//span[@itemprop="brand"]/a/text()').get()
        #if BRAND:
        #    item['brand']=BRAND
        #else:
        #    CAT=response.xpath('//span[@class="posted_in"]/a/text()').get()
        #    if CAT:
        #        item['brand']=(str(CAT).split(' Watches')[0]).strip()
        for color in COLORS:
            if color in str(NAME).lower() and item['dial_color']=='':
                item['dial_color']=str(color).title()
        if len(item['production_year'])>4:
            YEAR=str(item['production_year']).split()
            item['production_year']=''
            for Y in YEAR:
                if Get_Number_Only(Y)==Y and len(Get_Number_Only(Y))==4 and item['production_year']=='':
                    item['production_year']=Y
        if item['production_year']=='':
            STR_NAME=str(NAME).split()
            for rs in STR_NAME:
                if len(Get_Number_Only(rs))==4 and Get_Number_Only(rs)==rs and len(Get_Year(rs))==len(str(rs).strip()) and item['production_year']=='' and int(Get_Number_Only(rs))<=datetime.now().year and int(Get_Number_Only(rs))>1900:
                    item['production_year']=Get_Number_Only(rs)
            if item['production_year']=='':
                YEAR=''
                for rcs in STR_NAME:
                    if len(Get_Number_Only(rcs))>=4:
                        if '/' in rcs:
                            STR=str(rcs).split('/')
                            for rs in STR:
                                if len(Get_Number_Only(rs))==4 and Get_Number_Only(rs)==rs and int(rs)<=datetime.now().year and YEAR=='' and int(rs)>1900:
                                    YEAR=rs
                        elif len(Get_Number_Only(rcs))==4 and Get_Number_Only(rs)==rs and int(Get_Number_Only(rcs))<datetime.now().year and len(Get_Year(rcs))==len(str(rcs).strip()) and YEAR=='' and int(Get_Number_Only(rcs))>1900:
                            YEAR=rcs
                if YEAR!='':
                    item['production_year']=YEAR
        if 'mm' in str(NAME).lower():
            TXT=(str(NAME).lower()).split()
            for Txt in TXT:
                if 'mm' in Txt and len(Get_Number_Only(Txt))>0 and item['case_size']=='':
                    item['case_size']=Txt
        TXT_DES=cleanhtml(response.xpath('//div[contains(@id,"description")]').get())
        if 'mm' in TXT_DES:
            TXT=str(TXT_DES).split()
            for Txt in TXT:
                if 'mm' in Txt and len(Get_Number_Only(Txt))>1 and item['case_size']=='':
                    item['case_size']=Txt
        for k,v in item.items():
            if v=='':
                for K,V in IT.items():
                    if str(k).lower() in str(K).lower() and len(V)<50 and item[k]=='':
                        item[k]=V
                    if str(K).lower()=="case_diameter" and item['case_size']=='':
                        item['case_size']=V
        if len(Get_Number_Only(item['case_size']))>1:
            SIZE=Get_Number(str(item['case_size']))
            if len(SIZE)>=2 and len(SIZE)<=5:
                item['case_size']=SIZE+'mm'
            else:
                item['case_size']=''
        else:
            item['case_size']=''
        if item['brand']=='':
            for BR in BRANDS:
                if BR in str(NAME).upper() and item['brand']=='':
                    item['brand']=str(BR).title()
        item['model']=IT.get('model','')
        item['brand']=str(item['brand']).title()
        item['ref_number']=str(item['ref_number']).split('<')[0]
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
        scraped_data['title']=ITEM['name']
        scraped_data['sub_title']=''
        scraped_data['description']=DESC
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        if scraped_data['reference_number']=='' and (REF['model_number'] in scraped_data['model'] or REF['model_number'] in scraped_data['title']):
            scraped_data['reference_number']=REF['model_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=IT.get('bracelet material','')
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
