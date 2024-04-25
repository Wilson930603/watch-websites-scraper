# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime as dt

# useful for handling different item types with a single interface
import boto3, json, os, logging
from .settings import *
from .functions import *


def update_summary(spider, CRAWLING):
    for _ in range(60):
        try:
            summary_item_file = LOG_PATH + 'summary_' + spider.DATE_CRAWL.strftime('%a_%d-%m-%Y') + '.json'
            SUMMARY_ITEM = {}
            if os.path.exists(summary_item_file):
                SUMMARY_ITEM = json.loads(open(summary_item_file, 'r', encoding='utf-8').read())
            SUMMARY_ITEM.update(CRAWLING)
            open(summary_item_file, 'w', encoding='utf-8').write(json.dumps(SUMMARY_ITEM))
            break
        except Exception:
            random_sleep()
            continue


class CrawldataPipeline:
    def __init__(self):
        self.last_log_time = time.time()

    def open_spider(self, spider):

        # data_file_name = DATA_FILE_PATH+spider.name+'_'+spider.DATE_CRAWL.strftime('%a_%d-%m-%Y')+'.json'
        # self.data_file = open(data_file_name, 'w', encoding='utf-8')
        # self.data_file.write("[")

        # # boto3.set_stream_logger('',logging.ERROR)
        spider.sqs_client = boto3.client("sqs",aws_access_key_id=CONFIGS['aws_access_key_id'],aws_secret_access_key=CONFIGS['aws_secret_access_key'],region_name=CONFIGS['Region'])

        start_time = dt.now().strftime("%d %m %Y %H:%M:%S")
        spider.logger.info(f'********************* Start Time: {start_time}')
        for _ in range(60):
            try:
                self.CRAWLING = {spider.name: {'start_time': start_time,
                                               'finish_time': '-',
                                               'total': 0,
                                               'sent_messages': 0}}
                break
            except Exception:
                random_sleep()

    def close_spider(self, spider):
        # self.data_file.write("]")
        # self.data_file.close()

        close_time = dt.now().strftime("%d %m %Y %H:%M:%S")
        spider.logger.info(f'********************* Close Time: {close_time}')
        for _ in range(60):
            try:
                self.CRAWLING[spider.name]['finish_time'] = close_time
                break
            except Exception:
                random_sleep()

        update_summary(spider, self.CRAWLING)

    def process_item(self, item, spider):
        item['scraped_data']['price'] = str(item['scraped_data']['price'])

        # self.data_file.write(json.dumps(dict(item)) + ",\n")  # writing content in output file.

        for _ in range(60):
            try:
                self.CRAWLING[spider.name]['total'] += 1
                self.CRAWLING[spider.name][item['metadata']['search_query']] = self.CRAWLING[spider.name].get(item['metadata']['search_query'], 0) + 1
                break
            except Exception:
                random_sleep()

        try:
            KQ=send_message(spider.sqs_client,CONFIGS['QueueUrl'],item)
            if KQ:
                for _ in range(60):
                    try:
                        self.CRAWLING[spider.name]['sent_messages'] += 1
                        break
                    except Exception:
                        random_sleep()

        except Exception as e:
            spider.logger.info(f'ERROR SEND SQS MESSAGE OF EVENT_ID {item["metadata"]["event_id"]}, URL: {item["metadata"]["event_url"]}, {e}')
            try:
                log_error_message(spider.sns_client, CONFIGS['SNS_Topic_arn'], spider.logger, spider.name.title(),
                                  CONFIGS['QueueUrl'],
                                  'CRITICAL', 'SQS_ISSUE',
                                  f'Fail To Send SQS Message, {e}')
            except Exception as e:
                spider.logger.info(f'ERROR SEND SNS NOTIFICATION OF EVENT_ID {item["metadata"]["event_id"]}, URL: {item["metadata"]["event_url"]}, {e}')

        # update summary log
        if time.time() - self.last_log_time > (5*60):
            self.last_log_time = time.time()
            update_summary(spider, self.CRAWLING)
        return item
