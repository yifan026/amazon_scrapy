#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2018/2/8 下午 5:13

@author: YFC
"""

from scrapy.contrib.downloadermiddleware.useragent import UserAgentMiddleware
from middlewares.resource import USER_AGENT_LIST

import random


class RandomUserAgent(UserAgentMiddleware):
    def process_request(self, request, spider):
        ua = random.choice(USER_AGENT_LIST)
        request.headers.setdefault('User-Agent', ua)
