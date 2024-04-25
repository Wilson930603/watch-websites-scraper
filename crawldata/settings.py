import re
BOT_NAME = 'crawldata'
SPIDER_MODULES = ['crawldata.spiders']
NEWSPIDER_MODULE = 'crawldata.spiders'
URLLENGTH_LIMIT = 50000
ROBOTSTXT_OBEY = False
HTTPERROR_ALLOW_ALL=True
TELNETCONSOLE_ENABLED = False
#CONCURRENT_REQUESTS=1
#CONCURRENT_REQUESTS_PER_DOMAIN = 1
#CONCURRENT_REQUESTS_PER_IP = 1
DEFAULT_REQUEST_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1'}
ITEM_PIPELINES = {'crawldata.pipelines.CrawldataPipeline': 300}
CONFIG=re.split('\r\n|\n', open('config.txt').read())
CONFIGS={}
for rcs in CONFIG:
    rcs=str(rcs).strip()
    if '=' in rcs and not str(rcs).startswith('#'):
        rs=str(rcs).split('=')
        CONFIGS[rs[0]]=rs[1]
ITEM_DATA={'listing_uuid':'','brand':'','listing_url':'','ref_number':'','production_year':'','dial_color':'','bracelet_type':'','case_size':'','material':'','numerals':'','condition':'','new_preowned':'','complete_set':'','scraping_date':'','price':'','sale_date':'','model':'','bezel':''}
COLORS=['black','silver','gray','white','maroon','red','purple','fuchsia','green','lime','olive','yellow','navy','blue','teal','aqua','champagne','anthracite','white index']
NUMERALS=['Arabic Number','Arabic Numeral','Diamond Roman','Dot Marker','Index','Polished Marker','Rainbow Baguette','Roman','Stick']
BRACELET_TYPES=['Oyster','Jubilee','President','Presidential','Pearlmaster']
#LOG_ENABLED = True
#LOG_LEVEL='ERROR'
#LOG_FORMAT = '%(levelname)s: %(message)s'
LOG_PATH='./log/'