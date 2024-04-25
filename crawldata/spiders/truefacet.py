import time

import scrapy,json,re,string,os,platform
from crawldata.functions import *
from datetime import datetime
from urllib.parse import quote
from crawldata.settings import *
import boto3


class CrawlerSpider(scrapy.Spider):
    sns_client = boto3.client('sns', aws_access_key_id=CONFIGS['sns_access_key_id'],aws_secret_access_key=CONFIGS['sns_secret_access_key'],region_name=CONFIGS['Region'])
    name = 'truefacet'
    DATE_CRAWL=datetime.now()
    domain='https://www.truefacet.com'
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
                url='https://www.truefacet.com/catalogsearch/result/?q='+SEARCH+'&section=marketplace&p='+str(page)
                yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse(self, response):
        REF=response.meta['REF']
        SEARCH=response.meta['SEARCH']
        page=response.meta['page']
        Data=response.xpath('//div[@class="product-wrapper"]/div')
        for row in Data:
            ITEM={}
            ITEM['ID']=Get_Number_Only(row.xpath('.//div[@id]/@id').get())
            ITEM['brand']=row.xpath('.//div[@class="brand-name"]/text()').get()
            ITEM['search']=SEARCH
            ITEM['name']=row.xpath('.//div[@class="productTitle"]/text()').get()
            ITEM['own']=row.xpath('.//div[@class="n-worn"]/text()').get()
            PRICES=row.xpath('./div//div[@class="price-details"]//text()').getall()
            Prices=[]
            for i in range(len(PRICES)):
                PRICES[i]=str(PRICES[i]).strip()
                if PRICES!='' and len(cleanhtml(PRICES[i]))>0:
                    Prices.append(cleanhtml(PRICES[i]))
            ITEM['price']=Prices
            ITEM['url']=row.xpath('.//a/@href').get()
            file_url = str(ITEM['url']).lower()
            if file_url in self.done_urls:
                self.logger.info(f'URL SCRAPPED BEFORE: {file_url}')
                continue
            update_scrapped_urls(self.name, file_url, self.logger)
            self.done_urls.append(file_url)
            yield scrapy.Request(ITEM['url'],callback=self.parse_data,meta={'REF':REF,'ITEM':ITEM},dont_filter=True, priority=5 * (2 if REF['manufacturer'] in HIGH_PRIORITY else 1))
        # Next page
        if len(Data)>=60:
            page+=1
            url='https://www.truefacet.com/catalogsearch/result/?q='+SEARCH+'&section=marketplace&p='+str(page)
            yield scrapy.Request(url,callback=self.parse,meta={'REF':REF,'SEARCH':SEARCH,'page':page},dont_filter=True, priority=6 if REF['manufacturer'] in HIGH_PRIORITY else 1)
    def parse_data(self,response):
        REF=response.meta['REF']
        ITEM=response.meta['ITEM']
        BRANDS=[REF['manufacturer']]
        Data=response.xpath('//div[@class="pro-info"]//li')
        for row in Data:
            TITLE=row.xpath('./label/text()').get()
            VAL=row.xpath('./span/text()').get()
            if TITLE and VAL and len(TITLE)<30:
                ITEM['Detail_1_'+Get_Key_Str(TITLE)]=VAL
        Data=response.xpath('//div[@id="details"]/div[@class="text-content"]//text()').getall()
        DESC=cleanhtml(response.xpath('//div[@id="details"]/div[@class="text-content"]').get())
        NAME=response.xpath('//h1[@class="product-name-label"]/text()').get()
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
        item_tmp['url']=ITEM['url']
        LS_MAP={'brand':'brand','numerals':'indices','production_year':'year','dial_color':'color|dial','case_size':'case|size~diameter','material':'material','condition':'condition','model':'model','box':'box','papers':'papers'}
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
        item['listing_uuid']=ITEM['ID']
        item['ref_number']=''
        if 'Detail_2_reference_number' in ITEM:
            item['ref_number']=ITEM['Detail_2_reference_number']
        elif 'Detail_2_mpn' in ITEM:
            item['ref_number']=ITEM['Detail_2_mpn']
        elif 'Detail_2_model_number' in ITEM:
            item['ref_number'] = ITEM['Detail_2_model_number']
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
        item['listing_url']=str(ITEM['url']).lower()
        if len(item_tmp['production_year'])>0:
            for YEAR in item_tmp['production_year']:
                YR=(str(YEAR).replace("'s", '').replace("s", '').replace("-", ' ')).split()
                for Y in YR:
                    if Get_Number(Y)==Y and int(Y)<datetime.now().year and item['production_year']=='' and len(str(Y))==4:
                        item['production_year']=Y
        for nu in item_tmp['numerals']:
            if item['numerals']=='':
                item['numerals']=nu
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
        if 'Unworn' in ITEM['own'] or 'Never Worn' in ITEM['own']:
            item['new_preowned']='New'
        else:
            item['new_preowned']='Preowned'
        Box=False
        Papers=False
        if 'Original Box & Papers' in str(item_tmp['box']) or 'Original Box & Papers' in item_tmp['papers'] or 'Box and papers' in item_tmp['box'] or 'Box and papers' in item_tmp['papers']:
            Box=True
            Papers=True
        if 'yes' in str(item_tmp['box']).lower() or ('box' in str(item_tmp['box']).lower() and not 'No' in item_tmp['box'] and not 'non-' in str(item_tmp['box'])):
            Box=True
        if 'yes' in str(item_tmp['papers']).lower() or 'Papers' in str(item_tmp['papers']) or 'Certificate' in str(item_tmp['papers']):
            Papers=True
        if Box==True and Papers==True:
            item['complete_set']='TRUE'
        elif Box==True:
            item['complete_set']='box'
        elif Papers==True:
            item['complete_set']='papers'
        for rs in item_tmp['model']:
            if item['model']=='':
                item['model']=str(rs).strip()
        for SIZE in item_tmp['case_size']:
            if Get_Number(SIZE)==SIZE and item['case_size']=='':
                item['case_size']=SIZE+ ' mm'
            SIZE=(str(SIZE).lower()).replace(' mm', 'mm').replace('x', ' ')
            for rs in SIZE:
                if not rs in string.ascii_lowercase and not rs.isdigit() and rs!='.':
                    SIZE=str(SIZE).replace(rs, ' ')
            SIZE=str(SIZE).split()
            for rs in SIZE:
                if 'mm' in rs and Get_Number(rs)+'mm'==rs and item['case_size']=='':
                    item['case_size']=Get_Number(rs)+' mm'
        if item['case_size']=='':
            SIZE=(str(ITEM['name']).lower()).replace(' mm', 'mm').replace('x', ' ')
            for rs in SIZE:
                if not rs in string.ascii_lowercase and not rs.isdigit() and rs!='.':
                    SIZE=str(SIZE).replace(rs, ' ')
            SIZE=str(SIZE).split()
            for rs in SIZE:
                if 'mm' in rs and Get_Number(rs)+'mm'==rs and item['case_size']=='':
                    item['case_size']=Get_Number(rs)+' mm'
        item['price']=Get_Number(ITEM['price'][0])
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
        scraped_data['title']=NAME
        scraped_data['sub_title']=''
        scraped_data['description']=DESC
        scraped_data['manufacturer']=item['brand']
        scraped_data['model']=item['model']
        scraped_data['reference_number']=item['ref_number']
        if scraped_data['reference_number']=='' and (REF['model_number'] in scraped_data['model'] or REF['model_number'] in scraped_data['title']):
            scraped_data['reference_number']=REF['model_number']
        scraped_data['case_material']=item.get('material','')
        scraped_data['bracelet_material']=ITEM.get('Detail_2_bracelet','')
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
        DATASET['scraped_data']=scraped_data
        yield(DATASET)
