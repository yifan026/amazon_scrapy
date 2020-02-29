# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazontestItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    LastUpdateTime = scrapy.Field()
    Tag = scrapy.Field()

    # product page
    productTitle = scrapy.Field()
    productCreatedTime = scrapy.Field()  # no created time
    ProductContentJson = scrapy.Field()
    ProductContent = scrapy.Field()
    productBrand = scrapy.Field()  # company name
    productImage = scrapy.Field()
    productSrcName = scrapy.Field()  # amazon
    productFansCount = scrapy.Field()  # customer reviews + answered questions
    productLikeCount = scrapy.Field()  # stars
    productShareCount = scrapy.Field()
    productPrice = scrapy.Field()
    productSrcURL = scrapy.Field()
    ProductID = scrapy.Field()  # B00949CTQQ
    productCategory = scrapy.Field()
    descriptionJSON = scrapy.Field()
