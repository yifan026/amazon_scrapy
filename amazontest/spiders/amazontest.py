# -*- coding: utf-8 -*-

import logging
from datetime import datetime
import scrapy
from scrapy.http import Request
from amazontest.items import AmazontestItem
from ToolKit import *

from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

cur, conn = kit.mysql_db_info()
cc_pid = 0
# cur.execute('truncate crawler_log')


def insert_log(source_url, status_code, tag, category_node, response):
    sql_pattern = "INSERT INTO crawler_log(Source_URL,Source_Name,Status_Code,Tag,Category_Node,Update_Time,Response_HTML) " \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s)"

    ut = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.now())

    try:
        cur.execute(sql_pattern, (source_url, 'Amazon', status_code, tag, category_node, ut, response))
        conn.commit()
    except:
        conn.rollback()


def insert_prd(prd_info_object, table):
    keys = [k + ',' for k in prd_info_object.keys()]
    key_string = [''.join(keys[0::])][0][0:-1]
    values = '%s,' * len(keys)
    values = values[0:-1]

    sql_pattern = "INSERT INTO {} ({}) VALUES ({});".format(table, key_string, values)

    # ut = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.now())

    try:
        cur.execute(sql_pattern, tuple(o for o in prd_info_object.values(), ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.warning('Insert prd exception:')
        logging.warning(e)


def insert_category(category, pid):
    logging.warning('insert_category')
    logging.warning(category)

    get_parent_category_id = lambda c_dict, t: list(c_dict[t - 1].values())[1]
    split_dict = lambda sd: list(sd.values())[0]

    keys = list(split_dict(category).keys())
    key_string = [','.join(keys[0::])][0]
    values = '%s,' * len(keys)
    values = values[0:-1]
    # test.sort(key=lambda tup: tup[0])
    # category_executemany_list = [tuple(t.values()) for t in category[0]]
    category_executemany_list = [(v.values()) for k, v in category.items()]

    # sql_pattern = "INSERT INTO {} ({}) VALUES ({});".format(table, key_string, values)

    logging.warning('000000')
    logging.warning(category_executemany_list)
    logging.warning(category_executemany_list[0])

    sql_pattern = "insert into category ({}) values  ({});".format(key_string, values)

    # ut = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.now())

    for cel in category_executemany_list:
        try:
            logging.info('cel')
            logging.info(cel)
            cur.execute(sql_pattern, tuple(cel))
            # cur.executemany(sql_pattern, category_executemany_list)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.warning('Insert category exception:')
            logging.warning(e)

    # time.sleep(3)

    # conn.commit()
    # logging.warning(category_executemany_list[0][1])
    logging.warning('111111')

    c_id = list(category.values())[0]['Category_ID']

    logging.warning(c_id)

    cur.execute('select Category_Auto_Increment_ID  from category where Category_ID =\'' + str(c_id) + '\'')
    logging.warning('222222')

    cc_pid = cur.fetchall()
    logging.warning(cc_pid)

    cc_pid = cc_pid[0][0]

    # try:
    #     cc_pid = cur.fetchall()[0][0]
    # except Exception as e:
    #     logging.warning('Select Product_ID exception:')
    #     logging.warning(e)

    category_conjunction_product_executemany_list = [
        (pid, cc_pid + t - 1, '') if t == 1 and cc_pid else (pid, cc_pid + t - 1, get_parent_category_id(category, t))
        for t, (k, v) in enumerate(category.items(), 1)]

    key_string2 = 'FK_Product_ID,FK_Category_Auto_Increment_ID,Parent_Category_ID'
    sql_pattern2 = "insert into category_conjunction_product ({}) values  (%s,%s,%s);".format(key_string2)

    for cel in category_conjunction_product_executemany_list:

        try:
            cur.execute(sql_pattern2, tuple(cel))
            # cur.executemany(sql_pattern2, category_conjunction_product_executemany_list)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.warning('Insert category_conjunction_product exception:')
            logging.warning(e)


class AmazontestSpider(scrapy.Spider):
    name = "amazontest"
    allowed_domains = ["www.amazon.com"]

    prd_src_name = 'Amazon'

    faq_base_url = 'https://www.amazon.com/ask/questions/'

    category_base_url_1 = 'https://www.amazon.com/b/ref=s9_acss_bw_ct_325CT16_ct_1_h_w?_encoding=UTF8&node='
    category_base_url_2 = '&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-4'

    # productURL = [
    #     'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dbeauty&field-keywords=' + k + '&rh=n%3A3760911%2Ck%3A' + k
    #     for k in amazonItemList if not k.isdigit()]

    # productURL = [
    #     'https://www.amazon.com/s/ref=sr_pg_1?rh=n%3A3760911%2Ck%3A' + k + '&page=1&keywords=' + k + '&ie=UTF8'
    #     for k in
    #     amazonItemList]

    # categoryURL = [
    #     'https://www.amazon.com/b/ref=s9_acss_bw_ct_325CT16_ct_1_h_w?_encoding=UTF8&node=' + node + '&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-4'
    #     for node in
    #     amazonItemList if node.isdigit()]

    # str_list = productURL + categoryURL
    # str_list = categoryURL
    # str_list = categoryURL
    # start_urls = list(filter(None, str_list))

    # start_urls = single_product_URL
    # rules = [
    #     Rule(LinkExtractor(allow=(r'/s/ref=sr_pg_[1-2].*.page=[1-2].*')), callback='parse', follow=True),
    # ]

    MAX_PAGES = 1
    tag = ''

    faq_vote_count_list = []

    def __init__(self, item_node_start=None, item_node_end=None, *args, **kwargs):
        super(AmazontestSpider, self).__init__(*args, **kwargs)
        self.amazonItemList = kit.get_fans_list(4)[int(item_node_start):int(item_node_end)]

        # self.amazonItemList = kit.get_fans_list(4)[int(item_node_start)::]

        self.categoryURL = [
            'https://www.amazon.com/b/ref=s9_acss_bw_ct_325CT16_ct_1_h_w?_encoding=UTF8&node=' + node + '&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-4'
            for node in
            self.amazonItemList if node.isdigit()]

        self.str_list = self.categoryURL
        self.start_urls = list(filter(None, self.str_list))

        # logging.info('self.start_urls')
        # logging.info(self.start_urls)

        self.item_node_start = item_node_start
        self.item_node_end = item_node_end
        self._pages = 0
        self.captcha_pages_category = 0
        self.captcha_pages_product = 0
        self.failed_urls = []
        self.failed_category_urls = []
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)

    def get_node_num(self, response):

        if 'node=' in response.url:
            return response.url.split('node=')[-1].split('&')[0]
        elif 'n%3A' in response.url:
            return response.url.split('n%3A')[-1].split('&')[0]

    def get_err_url(self, response, err_url, tag, category_node):
        rs = response.status
        if response.status != (200 or 301):

            self.crawler.stats.inc_value(tag + '_failed_url_' + str(response.status))
            err_url.append(response.url)

        else:
            if 'Type the characters you see in this image:' in response.text:

                # if tag == 'Category':
                #     self.captcha_pages_category += 1
                # elif tag == 'Product_Page':
                #     self.captcha_pages_product += 1

                rs = 530
                self.crawler.stats.inc_value('Total_Captcha_Error_{}_{}'.format(tag, str(rs)))
                self.crawler.stats.inc_value('{}_{}_Captcha_url_{}'.format(tag, str(category_node), str(rs)))
            else:
                self.crawler.stats.inc_value(
                    '{}_{}_Successful_url_{}'.format(tag, str(category_node), str(response.status)))

        insert_log(response.url, rs, tag, category_node, response.text)

    def parse(self, response):

        node_num = self.get_node_num(response)
        self.tag = 'Category'
        self.get_err_url(response, self.failed_category_urls, self.tag, node_num)

        # self._pages += 1

        for pid, href in zip(response.xpath('//li[contains(@id,"result_")]/@data-asin').extract(),
                             response.xpath(
                                 '//li[contains(@id,"result_")]//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]/@href').extract()):
            product_url = response.urljoin(href)

            yield Request(product_url, callback=self.parse_product_detail, meta={'category_node': node_num})

        # 分頁處理
        try:
            next_page = response.selector.xpath('//a[@id="pagnNextLink"]/@href').extract()[0]
            page = next_page.split('page=')[-1].split('&')[0]
            logging.warning(node_num + ': next page is ' + page)
        except IndexError:
            next_page = ''

        if next_page:
            # if self._pages < self.MAX_PAGES:

            # print('\n{0}next page- {1} - {2}\n'.format(10 * '*', next_page, 10 * '*'))

            product_url = response.urljoin(next_page)

            # logging.warning('follow {}'.format(product_url))

            yield Request(product_url, self.parse)

        else:
            logging.warning(node_num + ': no next page!')

            # ---------------------------------------------------------------------------------------------------

    def parse_product_detail(self, response):

        # node_num = self.get_node_num(response)

        node_num = response.meta['category_node']
        self.tag = 'Product_Page'
        self.get_err_url(response, self.failed_urls, self.tag, node_num)

        item = AmazontestItem()

        # productTitleList = response.xpath(
        #     '//a[contains(@class,"a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal")]/@title').extract()

        # productPriceList=response.xpath('//span[contains(@class,"a-color-base sx-zero-spacing")]/@aria-label').extract()

        # product_url_list = response.xpath(
        #     '//a[contains(@class,"a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal")]/@href').extract()

        # ----------------------------------- Product Page Information ------------------------------------------------

        item['Tag'] = 'Product_Page'
        # item['product_title'] = response.xpath('//title/text()').extract()[0].split(' : ')[1]
        try:
            product_title = response.xpath('//span[@id="productTitle"]/text()').extract()[0]
        except IndexError:
            try:
                product_title = response.xpath('//meta[@name="title"]/@content').extract()[0].split(':')[1]
            except IndexError:
                product_title = ''

        prd_title = ' '.join(product_title.split())
        item['productTitle'] = prd_title

        # item['product_title'] = response.xpath('//meta[@name="title"]/@content').extract()[0].
        # split('Amazon.com : ')[-1].split(' : Beauty')[0]

        # try:
        #     item['productByAuthor'] = response.xpath('//div[@id="mbc"]/@data-brand').extract()[0]
        # except IndexError:
        #     product_by_author = response.xpath('//a[@id="brand"]/text()').extract()[0]
        #     item['productByAuthor'] = ' '.join(product_by_author.split())
        # except Exception:
        #     item['productByAuthor'] = response.xpath('//a[@id="bylineInfo"]/text()').extract()[0]
        # else:
        #     item['productByAuthor'] = ''

        try:
            brand = response.xpath('//div[@id="mbc"]/@data-brand').extract()[0]
        except IndexError:
            try:
                product_by_author = response.xpath('//a[@id="brand"]/text()').extract()[0]
                brand = ' '.join(product_by_author.split())
            except IndexError:
                try:
                    brand = response.xpath('//a[@id="bylineInfo"]/text()').extract()[0]
                except IndexError:
                    try:
                        brand = response.xpath('//a[@id="brandteaser"]/img/@alt').extract()[0]
                    except:
                        brand = ''

        item['productBrand'] = brand

        uk1 = response.url.split('/dp')[-1].split('/')[-1]

        try:
            uk2 = response.xpath(
                '//div[@class="a-section askDPSearchViewContent"]/form[@class="askDPSearchForm"]/@action').extract()[
                0].split('/')[-2]
        except IndexError:
            uk2 = ''
            # return

        if uk1 == uk2 or len(uk1) == len(uk2) == 10 or len(uk1) == 10:
            uk = uk1
        else:
            uk = uk2

        item['ProductID'] = uk

        item['productSrcURL'] = response.url

        item['productSrcName'] = self.prd_src_name

        # --------------------------------product description-start-----------------------------------------

        product_content_list = response.xpath(
            '//li[contains(@class,"showHiddenFeatureBullets")]/span/text()').extract()
        product_content_list = [' '.join(i.split()) for i in product_content_list]

        # {'About the product': product_content_list}

        product_description_content = response.xpath('//div[@id="product_description"]/p/text()').extract()
        if not product_description_content:
            product_description_content = response.xpath('//div[@id="productDescription"]/p/text()').extract()

        product_description_content = [' '.join(i.split()) for i in product_description_content]

        product_description_content_v2 = ['/n'.join(product_description_content[0::])]

        product_description_title = response.xpath('//div[@id="productDescription"]/h3/text()').extract()

        product_description = dict(zip(product_description_title, product_description_content))

        # {'Product description': product_description}

        # product_details #抓不到完全
        # response.xpath('//td[@class="bucket"]/div/ul/li/b/text()').extract()
        # response.xpath('//td[@class="bucket"]/h2/text()').extract()

        product_important_info_title = response.xpath(
            '//div[@id="importantInformation"]/div/div/h5/text()').extract()

        product_important_info_content = response.xpath(
            '//div[@id="importantInformation"]/div/div/text()').extract()

        product_important_info_content = [' '.join(i.split()) for i in product_important_info_content if
                                          ' '.join(i.split())]

        product_important_info = dict(zip(product_important_info_title, product_important_info_content))
        # {'Important information': product_important_info}

        product_content_json = {
            'About the product': product_content_list,
            'Product description': product_description,
            'Important information': product_important_info
        }

        item['ProductContentJson'] = {'Contents': str(product_content_json)}

        item['ProductContent'] = [
            '/n'.join(product_content_list + product_description_content_v2 + product_important_info_content)]

        # --------------------------------product description-end-----------------------------------------

        item['LastUpdateTime'] = kit.strf_time(datetime.now())

        category_node = response.xpath(
            '//ul[@class="a-unordered-list a-horizontal a-size-small"]/li/span/a/@href').extract()

        category_node_list = [
            r.split('node=')[-1] if r.split('node=')[-1].isdigit() else r.split('node=')[-1].split('&rh=')[0] for r in
            category_node]

        category_name = response.xpath(
            '//ul[@class="a-unordered-list a-horizontal a-size-small"]/li/span/a/text()').extract()

        category_name_list = [' '.join(r.split()) for r in category_name]

        category_json = dict(zip(category_name_list, category_node_list))

        category_name_before_last_one_name = response.xpath('//ul[@class="zg_hrsr"]/li[1]/span/a/text()').extract()

        category_name_before_last_one_node = response.xpath('//ul[@class="zg_hrsr"]/li[1]/span/a/@href').extract()

        category_name_before_last_one_node_list = [
            c.split('hpc/')[-1] if c.split('hpc/')[-1].isdigit() else c.split('/')[-1]
            if c.split('/')[-1].isdigit() else response.xpath('//ul[@class="zg_hrsr"]/li[1]/span/a/text()').extract()[0]
            for c in category_name_before_last_one_node]

        category_name_last_name = response.xpath('//ul[@class="zg_hrsr"]/li[1]/span/b/a/text()').extract()

        category_name_last_node = response.xpath('//ul[@class="zg_hrsr"]/li[1]/span/b/a/@href').extract()

        category_name_last_node_list = [c.replace('https://www.amazon.com/gp/bestsellers/', '').split('/')[1] for c in
                                        category_name_last_node]

        category_prd_details_json = dict(zip(category_name_before_last_one_name + category_name_last_name,
                                             category_name_before_last_one_node_list + category_name_last_node_list))

        category_for_sql = ''

        if category_json:
            item['productCategory'] = category_name_list
            category_for_sql = category_json
        else:
            if category_prd_details_json:
                item['productCategory'] = category_name_before_last_one_name + category_name_last_name
                category_for_sql = category_prd_details_json
            else:
                c = response.xpath('//*[@id="nav-search"]/form/div[1]/div/div/span/text()').extract()[0]
                category_for_sql = {c: c}
                item['productCategory'] = c

        # Source_Country = response.xpath('//span[@class="icp-color-base"]/text()').extract()[-1]

        try:
            customer_reviews_number = \
                response.xpath('//span[@id="acrCustomerReviewText"]/text()').extract()[0].split(' ')[0]
            customer_reviews_number = customer_reviews_number.replace(',', '')
        except:
            customer_reviews_number = 0

        try:
            answered_questions_number = \
                response.xpath('//a[@id="askATFLink"]/span/text()').extract()[0].split('answered')[0]
            answered_questions_number = answered_questions_number.replace(',', '').replace('+', '')
        except:
            answered_questions_number = 0

        prd_fans_conut = int(customer_reviews_number) + int(answered_questions_number)

        item['productFansCount'] = prd_fans_conut

        try:
            like_count_stars = \
                response.xpath('//span[@data-hook="rating-out-of-text"]/text()').extract()[0].split(' out of 5 stars')[
                    0]
        except:
            like_count_stars = 0

        item['productLikeCount'] = float(like_count_stars)

        item['productCreatedTime'] = kit.strf_time(datetime.now())

        product_image_list = response.xpath(
            '//li[contains(@class,"a-spacing-small item")]//span[contains(@class,"a-button-text")]//img/@src').extract()

        prd_imgs_list = [
            productImg.replace('SS40', 'SS650').replace('SX50_SY65_CR,0,0,50,65', 'SX640_SY640_CR,0,0,650,650').replace(
                'SR38,50', 'SR650,650') for productImg in product_image_list]

        item['productImage'] = prd_imgs_list

        # https://images-na.ssl-images-amazon.com/images/I/51FvfQXlgFL._SX50_SY65_CR,0,0,50,65_.jpg
        # https://images-na.ssl-images-amazon.com/images/I/51NFiT9kT%2BL._SX500_SY650_CR,0,0,500,650_.jpg

        # // element[contains( @ style, '/images') or ( @ class ='Image' and contains( @ src, '/images'))]

        # TODO 有些商品沒有顯示價格,或是要再進一部點擊才會顯示
        try:
            product_price = response.xpath(
                '//span[contains(@id,"priceblock_ourprice") or contains(@id,"priceblock_saleprice") or contains(@class,"a-size-large a-color-price")]/text()').extract()[
                0]
            prd_price = float(product_price.split('$')[-1].replace(',', ''))
        except:
            prd_price = -1

        item['productPrice'] = prd_price

        try:
            product_share_count = response.xpath('//span[@class="share_count_text"]/text()').extract()[0].replace(
                '+ Shares', '')

            product_share_count = product_share_count.replace(',', '')

            product_share_count_tmp = product_share_count.lower()

            if 'k' in product_share_count_tmp:
                product_share_count_tmp = int(product_share_count_tmp.replace('k', '')) * 1000

        except Exception as e:
            product_share_count_tmp = 0

        item['productShareCount'] = int(product_share_count_tmp)

        prd_description_dict = {}

        read_reviews_that_mention_list = response.xpath("//span[@data-hook ='lighthouse-term']/text()").extract()

        try:
            read_reviews_that_mention = [' '.join(i.split()) for i in read_reviews_that_mention_list][0::]
        except:
            read_reviews_that_mention = ''

        prd_details = response.xpath('//div[@class="content"]/ul/li/b/text()').extract()

        if 'UPC:' in prd_details:
            upc_list = response.xpath(
                '//div[@class="content"]/ul/li[' + str(prd_details.index('UPC:') + 1) + ']/text()').extract()[-1]
        else:
            upc_list = ''

        prd_description_dict.update(
            {"Read reviews that mention": read_reviews_that_mention, "category snapshot": category_for_sql,
             'UPC': upc_list})

        item['descriptionJSON'] = {'Descriptions': str(prd_description_dict)}

        prd_info = {}

        prd_info.update({'Product_ID': uk})
        prd_info.update({'Source_URL': response.url})
        prd_info.update({'Brand': brand})
        prd_info.update({'Content_Json': str(product_content_json)})
        prd_info.update({'Title': prd_title})
        prd_info.update({'Source_Name': self.prd_src_name})
        prd_info.update({'Price': prd_price})
        prd_info.update({'Img_URLs': str({str(t): d for t, d in enumerate(prd_imgs_list, 1)})})
        prd_info.update({'Last_Update_Time': kit.strf_time(datetime.now())})
        prd_info.update({'Latest_Like_Count': float(like_count_stars)})
        prd_info.update({'Latest_Share_Count': int(product_share_count_tmp)})
        prd_info.update({'Relationship_JSON': ''})
        prd_info.update({'Source_Fans_Count': prd_fans_conut})
        prd_info.update({'Description_JSON': str(prd_description_dict)})

        '''
        {'Brand': 'Majestic Pure',
         'Content_Json': "{'About the product': ['Majestic Pure Lavender Oil; blend of two (2) pure lavender oil; Lavandula augustifolia from Bulgaria and Lavandula hybrida from France; steam distilled; Therapeutic grade; Packaged in USA', 'AROMA: Best sampled on a test strip or tissue to allow the aroma to breathe, evolve and open up; top note with a strong aroma, it has a rich floral scent that is somewhat fruitier, mellower and less camphoraceous; Lavender oil is known to be helpful in soothing the mind and body when used in aromatherapy', 'SAFETY WARNING: For external use only. Other than aromatherapy, dilute with a carrier oil. For topical use, rub a very small amount on the inside of your elbow area to test for any allergic reaction before use, discontinue if irritation occurs. Keep out of the reach of pets. Essential oils can be potentially toxic to pets at certain concentrations; Avoid contact with eyes, keep out of the reach of children. If pregnant, consult with your health care provider before use', 'Majestic Pure lavender essential oil offers fresh fragrance that blends well with almost any other essence; the aroma becomes more flowery when it evolves', 'Delivered with a premium quality glass dropper for ease-of-use'], 'Product description': {}, 'Important information': {'Ingredients': 'Lavender Oil; blend of two (2) pure lavender oil - Lavandula augustifolia from Bulgaria and Lavandula hybrida from France', 'Directions': 'Use in diffuser or humidifier. For topical use, carefully dilute with a carrier oil before use. May cause skin irritation; a skin test is recommended prior to use. Avoid contact with eyes. For external use only.', 'Legal Disclaimer': 'This product is not intended to diagnose, mitigate, treat, cure or prevent any disease.'}}",
         'Img_URLs': "{'1': 'https://images-na.ssl-images-amazon.com/images/I/51erQINOsTL._SS650_.jpg', '2': 'https://images-na.ssl-images-amazon.com/images/I/414P4a-Q8WL._SS650_.jpg', '3': 'https://images-na.ssl-images-amazon.com/images/I/41rkCnMOcfL._SS650_.jpg', '4': 'https://images-na.ssl-images-amazon.com/images/I/41sBN%2B2geGL._SS650_.jpg', '5': 'https://images-na.ssl-images-amazon.com/images/I/41lwiAv2CEL._SS650_.jpg', '6': 'https://images-na.ssl-images-amazon.com/images/I/51M6VCikejL._SS650_.jpg', '7': 'https://images-na.ssl-images-amazon.com/images/I/51kJO6E6vhL._SS650_.jpg'}",
         'Last_Update_Time': '2017-12-12T15:41:30',
         'Latest_Like_Count': 1.5,
         'Latest_Share_Count': 22,
         'Price': 12.11,
         'Product_ID': 'B00TSTZQEY',
         'Relationship_JSON': '',
         'Source_Fans_Count': 1243,
         'Source_Name': 'Amazon',
         'Source_URL': 'https://www.amazon.com/Majestic-Pure-Lavender-Natural-Therapeutic/dp/B00TSTZQEY',
         'Title': 'Majestic Pure Lavender Oil, Pure and Natural with Therapeutic Grade, Premium Quality Blend of Lavender Oil, 4 fl. Oz'}
        '''

        insert_prd(prd_info, 'product')

        prd_history_info = {}

        prd_history_info.update({'FK_Product_ID': uk})
        prd_history_info.update({'Source_URL': response.url})
        prd_history_info.update({'Brand': brand})
        prd_history_info.update({'Content_Json': str(product_content_json)})
        prd_history_info.update({'Title': prd_title})
        prd_history_info.update({'Source_Name': self.prd_src_name})
        prd_history_info.update({'Price': prd_price})
        prd_history_info.update({'Img_URLs': str({str(t): d for t, d in enumerate(prd_imgs_list, 1)})})
        prd_history_info.update({'Last_Update_Time': kit.strf_time(datetime.now())})
        prd_history_info.update({'Latest_Like_Count': float(like_count_stars)})
        prd_history_info.update({'Latest_Share_Count': int(product_share_count_tmp)})
        prd_history_info.update({'Relationship_JSON': ''})
        prd_history_info.update({'Source_Fans_Count': prd_fans_conut})
        prd_history_info.update({'Description_JSON': str(prd_description_dict)})

        insert_prd(prd_history_info, 'product_update_histories')

        prd_category_dict = {t: {"Source_Name": response.url.split('/')[2], "Category_ID": v, "Category_Name": k,
                                 "Description": ''} for t, (k, v) in enumerate(category_for_sql.items(), 1)}

        if prd_category_dict:
            insert_category(prd_category_dict, uk)

        yield item

    def handle_spider_closed(self, spider, reason):  # added self
        # self.crawler.stats.set_value('Total crawl category', ','.join([str(self._pages)]))
        # self.crawler.stats.set_value('Total 530 category', ','.join([str(self.captcha_pages_category)]))
        # self.crawler.stats.set_value('Total 530 product', ','.join([str(self.captcha_pages_product)]))
        self.crawler.stats.set_value('Category_failed_urls', ','.join(
            [s.split('node=')[-1].split('&')[0] for s in spider.failed_category_urls]))
        conn.close()

    def process_exception(self, response, exception, spider):
        ex_class = "%s.%s" % (exception.__class__.__module__, exception.__class__.__name__)
        self.crawler.stats.inc_value('downloader/exception_count', spider=spider)
        self.crawler.stats.inc_value('downloader/exception_type_count/%s' % ex_class, spider=spider)
