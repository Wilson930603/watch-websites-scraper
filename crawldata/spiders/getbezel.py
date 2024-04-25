import time

import scrapy,json,re,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'getbezel'
    url='https://shop.getbezel.com'
    DATE_CRAWL=datetime.now()
    #custom_settings={'DOWNLOAD_DELAY':0.5}
    custom_settings={'DOWNLOAD_DELAY':1,'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.log'}
    #custom_settings={'CRAWLERA_ENABLED':True,'CRAWLERA_APIKEY':CONFIGS['CRAWLERA_APIKEY'],'AUTOTHROTTLE_ENABLED':False,'CRAWLERA_PRESERVE_DELAY':5,'DOWNLOADER_MIDDLEWARES':{'scrapy_crawlera.CrawleraMiddleware': 610},'LOG_FILE':LOG_PATH+name+'_'+DATE_CRAWL.strftime('%a')+'.log'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    cookies = {'dp1': 'bpbf/^%^230000000000c000e0000001000200000065f4f9e5^^u1p/QEBfX0BAX19AQA**67d62d65^^bl/US67d62d65^^','nonsession': 'BAQAAAYbdaqBpAAaAAMoAIGfWLWUwN2U3MzkyNjE4NjBhNDRkNTQxM2I2OGRmZmY2ZDUyNAAzAAll9PnlMDcwOTQsVVNBAMsAAmQTzW0yNkj0NcMZbM1UURUI7XrVe5+Ixmrr','__deba': 'aXYb059IDhuUQaIuxi5DYwlimoU86nt54vPK7t0M_syvkH_q9VOzkH1xprQEz4XYhZcfmgAixzFAncBdioUhpyPkFMTjHUSCj_SzzOqCp-Oap71ZQpdoxhVd1hEgxCAxQZpA20Earg7UTiugi7Fx7g==','__uzma': '71ce7bb4-e9ef-4531-8932-6040df2db191','__uzmb': '1675169847','__uzmc': '4181423869362','__uzmd': '1678776803','__uzme': '0262','__uzmf': '7f60009b1a55b6-da8f-4c5d-ab95-61291a926c9d16751698475583606955593-730feb329f7e29ad238','__ssds': '2','__ssuzjsr2': 'a9be0cd8e','__gads': 'ID=aae0c43ef61d01bd:T=1675169853:S=ALNI_MaiIeXVMIKVCZ1a7LH_q4wsDoIyeg','__gpi': 'UID=000009995392df39:T=1675169853:RT=1679017489:S=ALNI_Ma85TSn2st4bssowb1k_Ii6Phd7cA','cid': 'JClx981TAsARBMgS^%^232065445537','ns1': 'BAQAAAYbdaqBpAAaAANgATGX0+eVjNzJ8NjAxXjE2NzUxNjk5ODcwMDheXjFeM3wyfDV8NHw3fDExXjFeMl40XjNeMTJeMTJeMl4xXjFeMF4xXjBeMV42NDQyNDU5MDc1fJD4LRjwK3KgcH6IM0sKKQ8qz9w*','__gsas': 'ID=dd18647d7b9c2945:T=1675400604:S=ALNI_MZ2V5gthQE93UUoNT8VM5yFqjqXjQ','QuantumMetricUserID': '218458ced06aa6ab0c96f5796f1fcb9f','s': 'CgAD4ACBkFRflMDdlNzM5MjYxODYwYTQ0ZDU0MTNiNjhkZmZmNmQ1MjQA7gByZBUX5TMGaHR0cHM6Ly93d3cuZWJheS5jb20vc2NoL2kuaHRtbD9fZnJvbT1SNDAmX3Rya3NpZD1wMjM4MDA1Ny5tNTcwLmwxMzEyJl9ua3c9Um9sZXgrMTI0MzAwJl9zYWNhdD0wI2l0ZW0xZjM5M2RkNmMxB8CDfaY*','ebay': '^%^5Ejs^%^3D1^%^5Esbf^%^3D^%^2310000100000^%^5E','ak_bmsc': '983EB92E810391C7FA4C70E7441B585A~000000000000000000000000000000~YAAQtwyrcdrO1OOGAQAAwaI97ROiC6gZqDV3tB3P+NpyMwzIy3iwgRHoBa6VVazUfREzU0NVHAJS1nmWOugNS5EykSa+BK1hGwlv0ixuNGGZxzWhLZ1+8RB1VKXuE86yL/QATUkAAKvXMQKvyjEJgFEhm3bBO2QDqZX72ev0xIMD+/JhaZjxbcDcZtiKShRFlFqpKMFjycDDRs6miFxpC1gJHIAac4wZppnZN9H8ddR653zve8jjeuI1uDpSUx9fdqUIfC70ajuYHbcRLBawNccQhY1g9eVoMmnISBEJ5Sb3LQUO4PVTMBOM76R59+XIbqJ/4HmhOSwIbKgtIknRPFBe/TAXV7K4ANTnSPWWG/l52unXJPanm6njr+t4mRRKf+k7Q9D/YexO','bm_sv': '8F19E9E5C71FF4C3F771EF5E8C18753A~YAAQMywtF9N59smGAQAAufs+7RNfv5EMHeHlKgs7+5qXlxa9Y+TRTZy9W0V3/MezCwr/ugnYOr1AQsVdomzlOzx/b+98Fa08MPs59HzLXtj41DH3tcqGTijlOtRN4C4Ky0qK/5NlNiJVXUqXHO8rkwS5lZpEfVuozkZZkCskxDdjhtNPf1rnemr4Gpyw1gfdBohSrW6CeK+WxzJ7Z8MFmUdGB5AtA355GJR2dXfXlt/X0Kean21tndbmP4WmP2k=~1','ds2': 'sotr/b7pwxzzzzzzz^^','JSESSIONID': '921B5215954852CD17B8F96B8529A858'}
    IDS=[]
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
                page=0
                url='https://api.bezel.cloud/catalog/search?available=true&query='+SEARCH+'&page='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        BRANDS=[REF['manufacturer']]
        page=response.meta['page']
        Data=json.loads(response.text)
        for rows in Data['hits']:
            row=rows['object']
            item={}
            item.update(ITEM_DATA)
            item['listing_uuid']=str(row['id'])
            item['ref_number']=row['referenceNumber']
            item['numerals']=row['dial']['numerals']['name']
            item['complete set']='FALSE'
            item['bezel']=row['case']['bezelMaterial']['name']
            NAME=row['displayName']
            try:
                item['case _ize']=row['case']['size']+'mm'
            except:
                pass
            material=[]
            for rcss in row['case']['materials']:
                material.append(rcss['name'])
            item['material']=(', '.join(material))
            try:
                item['brand']=row['brand']['displayName']
            except:
                pass
            try:
                item['dial_color']=row['dial']['color']['name']
            except:
                pass
            try:
                item['bracelet_type']=row['bracelet']['style']['name']
            except:
                pass
            try:
                item['production_year']=row['releaseYear']
            except:
                pass
            if item['brand']=='':
                for BR in BRANDS:
                    if BR in str(NAME).upper() and item['brand']=='':
                        item['brand']=str(BR).title()
            item['brand']=str(item['brand']).title()
            item['listing_url']='https://shop.getbezel.com/watches/'+row['brand']['name']+'/'+(str((str(row['name']).split('/')[0]).strip()).replace(' ', '-')).lower()+'/ref-'+Get_Ref_Slug(row['referenceNumber'])+'/id-'+str(row['id'])
            file_url = item['listing_url'].replace('/id-', '/listing/id-')
            if file_url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {file_url}')
                continue

            update_scrapped_urls(self.name, file_url, self.logger)
            self.done_urls.append(file_url)
            yield scrapy.Request(item['listing_url'],callback=self.parse_price,meta={'item':item,'rows':row,'REF':REF},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        if len(Data['hits'])>0:
            page+=1
            url='https://api.bezel.cloud/catalog/search?type[]=MODEL&available=true&query='+SEARCH+'&page='+str(page)
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_price(self,response):
        ITEM=response.meta['item']
        rows=response.meta['rows']
        REF=response.meta['REF']
        HTML=response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        Data=json.loads(HTML)
        for row in Data['props']['pageProps']['listings']:
            item={}
            item.update(ITEM)
            try:
                item['production_year']=row['manufactureYear']
            except:
                try:
                    item['production_year']=row['purchaseYear']
                except:
                    pass
            item['price']=row['listedPriceCents']/100
            item['listing_uuid']+=('_'+str(row['id']))
            item['condition']=row['condition']
            if row['condition']=='UNWORN':
                item['new_preowned']='New'
            else:
                item['new_preowned']='Preowned'
            Box=False
            Papers=False
            for rs in row['accessories']:
                if 'box' in str(rs['name']).lower():
                    Box=True
                if 'papers' in str(rs['name']).lower():
                    Papers=True
            if Box==True and Papers==True:
                item['complete_set']='TRUE'
            elif Box==True:
                item['complete_set']='box'
            elif Papers==True:
                item['complete_set']='papers'
            item['listing_url']=str(item['listing_url']).split('/id-')[0]+'/listing/id-'+str(row['id'])
            #Scraped data
            DATASET={}
            metadata={}
            metadata['marketplace']=str(self.name).title()
            metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
            metadata['search_type']='listing'
            metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
            metadata['event_time']=''
            metadata['event_id']=str(item['listing_uuid'])
            metadata['event_url']=item['listing_url']
            scraped_data={}
            scraped_data['price']=item['price']
            scraped_data['title']=rows['name']
            scraped_data['sub_title']=''
            scraped_data['description']=rows.get('description','')
            scraped_data['manufacturer']=item['brand']
            scraped_data['model']=row['model']['name']
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
            (scraped_data['miscellaneous']).update(rows)
            (scraped_data['miscellaneous']).update(row)
            DATASET['metadata']=metadata
            DATASET['scraped_data']=scraped_data
            yield(DATASET)
