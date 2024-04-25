import time

import scrapy,json,re,string,platform,os
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'catawiki'
    domain='https://www.catawiki.com'
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
                url='https://www.catawiki.com/buyer/api/v1/search?locale=en&per_page=24&sort=relevancy_desc&q='+SEARCH+'&page='+str(page)
                yield scrapy.Request(url,callback=self.filter_watches,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)

    def filter_watches(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        Data=json.loads(response.text)
        FILTER=''
        if Data.get('total', 0) > 0:
            for filter in Data.get('facets', []):
                if 'Category' == filter.get('name'):
                    for category in filter.get('values', []):
                        if 'Watches' in category.get('label', '') and 'Accessories' not in category.get('label', ''):
                            FILTER += '&filters%5Bl2_categories%5D%5B%5D=' + category.get('position', '')

            url = 'https://www.catawiki.com/buyer/api/v1/search?locale=en&per_page=24&sort=relevancy_desc&q='+SEARCH+'&page='+str(page)+FILTER
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page,'FILTER':FILTER},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)

    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        FILTER=response.meta['FILTER']
        Data=json.loads(response.text)
        for ITEM in Data['lots']:
            if ITEM['url'] in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {ITEM["url"]}')
                continue

            update_scrapped_urls(self.name, ITEM['url'], self.logger)
            self.done_urls.append(ITEM['url'])
            yield scrapy.Request(ITEM['url'],
                                 callback=self.parse_data,
                                 priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1),
                                 meta={'REF':REF,'ITEM':ITEM},
                                 dont_filter=True)
        # Next page
        if len(Data)>=24:
            page+=1
            url='https://www.catawiki.com/buyer/api/v1/search?locale=en&per_page=24&sort=relevancy_desc&q='+SEARCH+'&page='+str(page)+FILTER
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page,'FILTER':FILTER},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
            
    def parse_data(self,response):
        REF=response.meta['REF']
        BRANDS=[REF['manufacturer']]
        ITEM=response.meta['ITEM']
        ITEM['description']=cleanhtml(response.xpath('//div[@class="lot-info-description__description"]').get())
        Data=response.xpath('//div[@class="be-lot-specification"]')
        for row in Data:
            TITLE=row.xpath('./span[contains(@class,"be-lot-specification__name")]/text()').get()
            VAL=row.xpath('.//span[not(contains(@class,"be-lot-specification__name")) and string-length(text())>0]/text()').get()
            if len(TITLE)<30:
                ITEM['Detail_1_'+Get_Key_Str(TITLE)]=str(VAL).strip()
        Data=response.xpath('//div[@class="lot-info-description__description"]//text()').getall()
        Dataset=[]
        for i in range(len(Data)):
            txt=str(Data[i]).strip()
            if txt!='':
                Dataset.append(txt)
        for rs in Dataset:
            rcs=str(rs).split(':')
            if len(rcs)>1 and len(str(rcs[1]).strip())>1 and len(str(rcs[0]).strip())<30:
                ITEM['Detail_2_'+Get_Key_Str(rcs[0]).strip()]=str(rcs[1]).strip()
        item_tmp={}   
        item_tmp['SHEET']='tmp'
        item_tmp['url']=ITEM['url']
        LS_MAP={'brand':'brand','production_year':'year~period','dial_color':'color|dial','case_size':'case|size~diameter','material':'material','condition':'condition','model':'model','box':'box~extras','papers':'papers~extras'}
        for K,VL in LS_MAP.items():
            item_tmp[K]=[]
            VS=str(VL).split('~')
            for vs in VS:
                V=str(vs).split('|')
                for k,v in ITEM.items():
                    k=str(k).replace('Detail_1_', '').replace('Detail_2_', '')
                    if len(V)==1:
                        if V[0] in k:
                            if K in item_tmp and not v in item_tmp[K]:
                                item_tmp[K].append(str(v).strip())
                            else:
                                item_tmp[K]=[str(v).strip()]
                    else:
                        if (V[0] in k and V[1] in k) or str(V[0]).lower()==str(k).lower() or str(V[1]).lower()==str(k).lower():
                            if K in item_tmp and not v in item_tmp[K]:
                                item_tmp[K].append(str(v).strip())
                            else:
                                item_tmp[K]=[str(v).strip()]
        #print(ITEM)
        item={}
        item.update(ITEM_DATA)
        item['listing_uuid']=ITEM['id']
        for i in range(len(item_tmp['brand'])):
            item_tmp['brand'][i]=str(item_tmp['brand'][i]).title()
        BR=list(set(BRANDS)&set(item_tmp['brand']))
        if len(BR)>0:
            item['brand']=BR[0]
        if item['brand']=='':
            for br in BRANDS:
                for Br in item_tmp['brand']:
                    if br in Br and item['brand']=='':
                        item['brand']=br
        if item['brand']=='':
            for br in BRANDS:
                for Br in ITEM.values():
                    Brs=(str(Br).title()).split()
                    if br in Brs and item['brand']=='':
                        item['brand']=br
        if item['brand']!='':
            item['listing_url']=ITEM['url']
            item['ref_number']=((re.split('\"|\'|\(',ITEM.get('Detail_1_reference_number',''))[0]).replace('Ref.', '').replace('ref.', '').replace('No:', '')).strip()
            if len(item_tmp['production_year'])>0:
                for YEAR in item_tmp['production_year']:
                    YR=(str(YEAR).replace("'s", '').replace("s", '').replace("-", ' ')).split()
                    for Y in YR:
                        if Get_Number(Y)==Y and len(str(Y))==4 and int(Y)<datetime.now().year and item['production_year']=='':
                            item['production_year']=Y
            for dc in item_tmp['dial_color']:
                for nu in NUMERALS:
                    if nu in dc and item['numerals']=='':
                        item['numerals']=nu
                dc=str(dc).lower()
                for cl in COLORS:
                    if cl in dc and item['dial_color']=='':
                        item['dial_color']=str(cl).title()
            for Type in BRACELET_TYPES:
                for v in ITEM.values():
                    if v and Type in str(v).title() and item['bracelet_type']=='':
                        item['bracelet_type']=Type
            item['condition']='; '.join(item_tmp['condition'])
            item['material']='; '.join(item_tmp['material'])
            if 'Unworn' in item_tmp['condition']:
                item['new_preowned']='New'
            else:
                item['new_preowned']='Preowned'
            Box=False
            Papers=False
            if 'yes/yes' in str(item_tmp['box']) or 'yes/yes' in item_tmp['papers'] or 'full set' in str(item_tmp['box']).lower() or 'full set' in str(item_tmp['papers']).lower():
                Box=True
                Papers=True
            if 'yes' in str(item_tmp['box']).lower() or 'Box' in str(item_tmp['box']):
                Box=True
            if 'yes' in str(item_tmp['papers']).lower():
                Papers=True
            if Box==True and Papers==True:
                item['complete set']='TRUE'
            elif Box==True:
                item['complete set']='box'
            elif Papers==True:
                item['complete set']='papers'
            item['box']=Box
            item['papers']=Papers
            for rs in item_tmp['model']:
                if item['model']=='':
                    item['model']=rs
                elif len(rs)<len(item['model']):
                    item['model']=rs
            for SIZE in item_tmp['case_size']:
                SIZE=(str(SIZE).lower()).replace(' mm', 'mm').replace('x', ' ')
                for rs in SIZE:
                    if not rs in string.ascii_lowercase and not rs.isdigit() and rs!='.':
                        SIZE=str(SIZE).replace(rs, ' ')
                SIZE=str(SIZE).split()
                for rs in SIZE:
                    if 'mm' in rs and Get_Number(rs)+'mm'==rs and item['case_size']=='':
                        item['case_size']=Get_Number(rs)+' mm'
            #https://www.catawiki.com/buyer/api/v3/bidding/lots?ids=68803441
            url='https://www.catawiki.com/buyer/api/v3/bidding/lots?ids='+str(item['listing_uuid'])
            yield scrapy.Request(url,
                                 callback=self.parse_price,
                                 priority=10,
                                 meta={'item':item,'ITEM':ITEM,'REF':REF})

    def parse_price(self,response):
        REF=response.meta['REF']
        item=response.meta['item']
        ITEM=response.meta['ITEM']
        Data=json.loads(response.text)
        item['price']=Data['lots'][0]['current_bid_amount']['USD']
        if str(Data['lots'][0]['bidding_end_time']).split('T')[0]==self.DATE_CRAWL:
            item['sale_date']=str(Data['lots'][0]['bidding_end_time']).split('T')[0]
        #Scraped data
        DATASET={}
        metadata={}
        metadata['marketplace']=str(self.name).title()
        metadata['search_query']=REF['manufacturer']+' '+REF['model_name']+' '+REF['model_number']
        metadata['search_type']='offer'
        metadata['scrape_time']=self.DATE_CRAWL.strftime('%Y-%m-%d %H:%M:%S')
        metadata['event_time']=item['sale_date']
        if metadata['event_time']!='':
            metadata['search_type']='sale'
        metadata['event_id']=str(item['listing_uuid'])
        # metadata['event_url']=response.url
        metadata['event_url']=item['listing_url']
        DATASET['metadata']=metadata
        scraped_data={}
        scraped_data['price']=item['price']
        scraped_data['title']=ITEM['title']
        scraped_data['sub_title']=ITEM['subtitle']
        scraped_data['description']=ITEM['description']
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=ITEM.get('bracelet','')
        scraped_data['dial_color']=item['dial_color']
        scraped_data['dial_indicies']=item['numerals']
        scraped_data['bezel']=item['bezel']
        scraped_data['case_size']=item['case_size']
        scraped_data['condition']=item['condition']
        scraped_data['box']=item['box']
        scraped_data['papers']=item['papers']
        scraped_data['production_year']=item['production_year']
        scraped_data['miscellaneous']={}
        (scraped_data['miscellaneous']).update(ITEM)
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
