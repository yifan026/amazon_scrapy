#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2018/2/8 下午 5:21

@author: YFC
"""

from middlewares.resource import PROXIES
import random


class RandomProxy(object):
    def process_request(self, request, spider):
        proxy = random.choice(PROXIES)
        request.meta['proxy'] = proxy
