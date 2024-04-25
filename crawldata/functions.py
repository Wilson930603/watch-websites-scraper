import hashlib,re,requests,json
import random
import time

from lxml.html.clean import Cleaner
from datetime import datetime

cleaner = Cleaner(
    meta=True,
    scripts=True,
    embedded = True,
    javascript=True,
    style=True,
    inline_style=True,
    comments=True,
)

__version__ = '0.1.2'
class TrackerBase(object):
    def on_start( response):
        pass
    def on_chunk( chunk):
        pass
    def on_finish(self):
        pass
class ProgressTracker(TrackerBase):
    def __init__( progressbar):
        progressbar = progressbar
        recvd = 0
    def on_start( response):
        max_value = None
        if 'content-length' in response.headers:
            max_value = int(response.headers['content-length'])
        progressbar.start(max_value=max_value)
        recvd = 0
    def on_chunk( chunk):
        recvd += len(chunk)
        try:
            progressbar.update(recvd)
        except ValueError:
            # Probably the HTTP headers lied.
            pass
    def on_finish(self):
        progressbar.finish()
class HashTracker(TrackerBase):
    def __init__( hashobj):
        hashobj = hashobj
    def on_chunk( chunk):
        hashobj.update(chunk)
def get_case_size(txt):
    KQ=''
    i=0
    while i<len(txt) and (str(txt[i]).isdigit() or str(txt[i])=='.'):
        KQ+=txt[i]
        i+=1
    return KQ
def download(url, target, proxy=None , headers=None, trackers=()):
    if headers is None:
        headers = {}
    headers.setdefault('user-agent', 'requests_download/'+__version__)
    if not proxy is None and ':' in proxy:
        proxies={'http':proxy,'https':proxy}
        r = requests.get(url, proxies=proxies, headers=headers, stream=True)
    elif not proxy is None:
        proxy_host = "proxy.crawlera.com"
        proxy_port = "8010"
        proxy_auth = proxy
        proxies = {"http": "http://{}@{}:{}/".format(proxy_auth, proxy_host, proxy_port),}
        r = requests.get(url, proxies=proxies, headers=headers, stream=True, verify=False, timeout=20)
    else:
        r = requests.get(url, headers=headers, stream=True)
    r.raise_for_status()
    for t in trackers:
        t.on_start(r)
    with open(target, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                for t in trackers:
                    t.on_chunk(chunk)
    for t in trackers:
        t.on_finish()
def translate(text,fromlag,tolang):
    data = {'text': text,'gfrom': fromlag,'gto': tolang}
    response = requests.post('https://www.webtran.eu/gtranslate/', data=data)
    return(response.text)
def Get_Number(xau):
    KQ=re.sub(r"([^0-9.])","", str(xau).strip())
    return KQ
def Get_Number_Only(xau):
    KQ=re.sub(r"([^0-9])","", str(xau).strip())
    return KQ
def Get_Ref_Slug(xau):
    KQ=re.sub(r"([^0-9])","-", str(xau).strip())
    return KQ
def Get_Year(xau):
    KQ=re.sub(r"([^0-9*!()])","", str(xau).strip())
    return KQ
def Get_String(xau):
    KQ=re.sub(r"([^A-Za-z_])","", str(xau).strip())
    return KQ
def Get_KEY(xau):
    KQ=re.sub(r"([^A-Za-z0-9_])","-", str(xau).strip())
    return KQ
def cleanhtml(raw_html):
    if raw_html:
        raw_html = cleaner.clean_html(raw_html)
        raw_html=str(raw_html).replace('</',' ^</')
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        cleantext=(' '.join(cleantext.split())).strip()
        cleantext=str(cleantext).replace(' ^','^').replace('^ ','^')
        while '^^' in cleantext:
            cleantext=str(cleantext).replace('^^','^')
        cleantext=str(cleantext).replace('^',' \n ')
        return cleantext.strip()
    else:
        return ''
def kill_space(xau):
    xau=str(xau).replace('\t','').replace('\r','').replace('\n',', ')
    xau=(' '.join(xau.split())).strip()
    return xau
def key_MD5(xau):
    xau=(xau.upper()).strip()
    KQ=hashlib.md5(xau.encode('utf-8')).hexdigest()
    return KQ
def get_item_from_json(result,item,space):
    if isinstance(item,dict):
        for k,v in item.items():
            if isinstance(v,dict) or isinstance(v,list):
                if space=='':
                    get_item_from_json(result,v,k)
                else:
                    get_item_from_json(result,v,space+'.'+k)
            else:
                if space=='':
                    result[k]=v
                else:
                    result[space+'.'+k]=v
    else:
        for i in range(len(item)):
            k=str(i)
            v=item[i]
            if isinstance(v,dict) or isinstance(v,list):
                if space=='':
                    get_item_from_json(result,v,k)
                else:
                    get_item_from_json(result,v,space+'.'+k)
            else:
                if space=='':
                    result[k]=v
                else:
                    result[space+'.'+k]=v
    return result
# Do with aws sqs
def send_message(sqs_client,QueueUrl,message):
    MessageAttributes={'Title': {'DataType': 'String','StringValue': 'Watch project'},'Author': {'DataType': 'String','StringValue': 'Tung Tran'},'Date_create': {'DataType': 'String','StringValue': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}}
    response = sqs_client.send_message(QueueUrl=QueueUrl,MessageAttributes=MessageAttributes,MessageBody=json.dumps(message))
    KQ=response['MessageId']
    return KQ
def Get_Key_String(xau):
    KQ=re.sub(r"([^A-Za-z0-9])","_", str(xau).strip())
    return KQ
def Get_Key_Str(xau):
    KQ=re.sub(r"([^A-Za-z0-9])","_", (str(xau).lower()).strip())
    while '__' in KQ:
        KQ=str(KQ).replace('__', '_')
    return KQ
def log_error_message(sns_client, TopicArn, logger, marketplace, event_url, error_severity, error_code, error_description):
    MessageDict = {"metadata":
        {
            "error_id": str(random.randint(10 ** 12, 10 ** 13 - 1)),
            "error_time": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            "environment": 'DEV' if 'dev' in TopicArn else 'PROD',
            "marketplace": marketplace,
            "last_scraped_event_url": event_url,
        },
        "error_data":
            {
                "error_severity": error_severity,
                "error_code": error_code,
                "error_description": error_description,
            }
    }
    message_json = json.dumps(MessageDict)
    logger.info(message_json)
    try:
        KQ = sns_client.publish(TopicArn=TopicArn, Message=message_json)
        logger.info(f"Message Sent ID={KQ['MessageId']}")
    except Exception as e:
        logger.info(f'ERROR SEND SNS MESSAGE OF {message_json}, {e}')
def random_sleep():
    time.sleep(round(random.uniform(0.5, 1.0), 1))
def update_scrapped_urls(spider_name, url, logger):
    for _ in range(60):
        try:
            with open('./scraped_urls/' + str(spider_name).title() + '_urls', 'a', encoding='utf-8') as url_file:
                url_file.write(url + '\n')
            break
        except Exception:
            random_sleep()
    else:
        logger.info(f'ERROR ADD SCRAPPED URL: {url}')
def get_scrapped_urls(spider_name, logger):
    url_file_path = './scraped_urls/' + str(spider_name).title() + '_urls'
    try:
        with open(url_file_path, 'r', encoding='utf-8') as ifile:
            done_urls = list(set([url.strip() for url in ifile.readlines() if url.strip()]))
    except Exception as e:
        logger.info(f'ERROR GETTING OLD SCRAPED URLs, {e}')
        done_urls = list()

    if done_urls:
        logger.info(f'Number of previously scraped urls = {len(done_urls)}')
        try:
            with open(url_file_path, 'w', encoding='utf-8') as url_file:
                for url in done_urls:
                    url_file.write(url + '\n')
        except Exception:
            pass
    return done_urls
