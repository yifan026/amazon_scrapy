# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
from models.ToolKit import kit
# from elasticsearch import Elasticsearch, TransportError
# import configparser
# import os
from termcolor import colored
import time
import copy

isLocalHost = int(kit.config.get('ESdb', 'localhost'))
host = kit.config.get('ESdb', 'host')
ESPort = kit.config.get('ESdb', 'port')

docType = 'Amazon'
indexName = 'crawler_product'


class AmazontestPipeline(object):
    def __init__(self):
        self.es, self.ho = kit.get_elasticsearch(isLocalHost, host, ESPort)

    def process_item(self, item, spider):
        es2, ho2 = self.es, self.ho

        try:
            es_current_count = es2.count(index=indexName, doc_type=docType)['count']
        except TransportError as e:
            logging.warning(e.info)
            es_current_count = 0

        doc_key = (
            'Source_Name', 'Source_URL', 'Source_Fans_Count', 'Brand', 'Created_Time', 'Title', 'Content_Json',
            'Content', 'Img_URLs', 'Last_Update_Time', 'Latest_Like_Count', 'Product_ID', 'Category', 'Price',
            'Latest_Share_Count', 'Page_Tag', 'Description_JSON')

        product_info_value = (item['productSrcName'], item['productSrcURL'], item['productFansCount'],
                              item['productBrand'], item['productCreatedTime'], item['productTitle'],
                              item['ProductContentJson'], item['ProductContent'], item['productImage'],
                              item['LastUpdateTime'], item['productLikeCount'], item['ProductID'],
                              item['productCategory'], item['productPrice'], item['productShareCount'],
                              item['Tag'], item['descriptionJSON'])

        doc_product_info = dict(zip(doc_key, product_info_value))

        res = es2.search(index=indexName, doc_type=docType,
                         body={"query": {"match": {"Product_ID": item['ProductID']}}, "size": "10000"})

        # need_delete_doc = [docType + "_" + str(d) for d in sorted([int(r['_id'][7:]) for r in res['hits']['hits']])]
        need_delete_doc = [r['_id'] for r in res['hits']['hits']]

        if len(need_delete_doc) > 1:
            s3 = 1
            while s3 != 0:
                try:
                    res3 = es2.delete_by_query(index=indexName, doc_type=docType,
                                               body={
                                                   "query": {"ids": {"type": docType, "values": need_delete_doc[1:]}}},
                                               request_timeout=30)
                    s3 = res3['deleted']
                except Exception as e:
                    time.sleep(1.5)

        if any(item['ProductID'] in s for s in [d['_source']['Product_ID'] for d in res['hits']['hits']]):

            ida = res['hits']['hits'][0]['_id']

            doc_old = copy.deepcopy(res['hits']['hits'][0]['_source'])

            if doc_old:
                doc_after_diff = kit.doc_diff(doc_old, doc_product_info)

            if doc_after_diff:
                try:
                    es2.index(index=indexName, doc_type=docType, id=str(ida), body=doc_product_info)
                    # es.update(index=indexName, doc_type=docType, id=str(ida), body=doc)
                except TransportError as e:
                    print(e.info)
                    # print(colored('same post {} ,the diff is {} ,overwrite the data',
                    #               'red'.format(str(ida), doc_after_diff)))
            else:
                print(colored('same post ' + str(ida) + ' ,nothing to do!', 'red'))

        else:
            try:
                es_current_count += 1
                es2.index(index=indexName, doc_type=docType,
                          # id=docType + '_' + str(es_current_count),
                          body=doc_product_info)
            except TransportError as e:
                print(e.info)
                es_current_count -= 1
                # print(str(es_current_count) + ' datas - *** ' + item['content'][0:50] + " *** insertion to " + ho2 + " done!")

        return item

        # 開始一個爬蟲,在這做啟動一些起始動作

    def open_spider(self, spider):
        pass

        # 終止前要做的動作

    def close_spider(self, spider):
        pass
