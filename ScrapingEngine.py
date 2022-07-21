import argparse 
import requests
import json
import sys
import time
import maya
import socket

import urllib.parse
import urllib.request

from pytz import timezone
import datetime
KST = timezone('Asia/Seoul')

import os
import time
from itertools import repeat
import socket
import multiprocessing
# We must import this explicitly, it is not imported by the top-level
# multiprocessing module.
import multiprocessing.pool
import time

# 로그 생성
import logging
logger = logging.getLogger()
logger.setLevel(logging.CRITICAL)
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler = logging.FileHandler('log.txt')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


## import file
import AuthenticationManager
import GetCursor

## set kafka
from kafka import KafkaProducer




class ScrapingEngine(object):
    def __init__(self, query, language, x_guest_token):
        ## Setting query
        self.query = query
        
        ## Setting authorization keysets
        self.x_guest_token = x_guest_token 
        
        ## Setting init  
        self.cursor = None
        self.totalcount = 0
        
        ## Setting base url
        self.base_url = "https://twitter.com/search?q="
        
        ## Setting kafka
        self.producer = KafkaProducer(acks=0, compression_type='gzip', api_version=(0, 10, 1), bootstrap_servers=['117.17.189.205:9092','117.17.189.205:9093','117.17.189.205:9094'])
        
        ## Setting Language type
        self.accept_language = language
        self.x_twitter_client_language = language


    def set_search_url(self):
        self.url = self.base_url + self.query +"&src=typed_query&f=live"
        
        return self.url


    def set_token(self):
        print(self.accept_language, self.query, "retry token")
        while True:
            x_guest_token = AuthenticationManager.get_x_guest_token()
            if x_guest_token != None :
                break
            continue
        
        return x_guest_token
    
    
    def start_scraping(self):
        ## get URL
        self.url = self.set_search_url()

        request_count = 0 

        while (True):
            request_count = request_count + 1

            if request_count == 100 :
                request_count = 0
                self.x_guest_token = self.set_token()
            
            ## setting header
            self.headers = {
                    'Accept': '*/*',
                    'Accept-Language': self.accept_language,
                    'x-guest-token': self.x_guest_token,
                    'x-twitter-client-language': self.x_twitter_client_language,
                    'x-twitter-active-user': 'yes',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
                    'Referer': self.url,
                    'Connection': 'keep-alive',
                    'TE': 'trailers',
            }
            
            ## setting parameters
            self.params = (
                    ('include_profile_interstitial_type', '1'),
                    ('include_blocking', '1'),
                    ('include_blocked_by', '1'),
                    ('include_followed_by', '1'),
                    ('include_want_retweets', '1'),
                    ('include_mute_edge', '1'),
                    ('include_can_dm', '1'),
                    ('include_can_media_tag', '1'),
                    ('skip_status', '1'),
                    ('cards_platform', 'Web-12'),
                    ('include_cards', '1'),
                    ('include_ext_alt_text', 'true'),
                    ('include_quote_count', 'true'),
                    ('include_reply_count', '1'),
                    ('tweet_mode', 'extended'),
                    ('include_entities', 'true'),
                    ('include_user_entities', 'true'),
                    ('include_ext_media_color', 'true'),
                    ('include_ext_media_availability', 'true'),
                    ('send_error_codes', 'true'),
                    ('simple_quoted_tweet', 'true'),
                    ('q', self.query+" -is:retweet"),
                    ('tweet_search_mode', 'live'),
                    ('count', '40'),
                    ('query_source', 'typed_query'),
                    ('pc', '1'),
                    ('spelling_corrections', '1'),
                    ('ext', 'mediaStats,highlightedLabel'),
                    ('cursor', self.cursor ), ## next cursor range
            )
            
            ## api requests 
            try:
                self.response = requests.get(
                        'https://twitter.com/i/api/2/search/adaptive.json', 
                        headers=self.headers,
                        params=self.params,
                        timeout=2
                        )
                
                self.response_json = self.response.json()
            except Exception as ex:
                ## If API is restricted, request to change Cookie and Authorization again
                result_print = "lan_type={0:<10}|query={1:<20}|change Cookie&Authorization| error={2}|".format(
                    self.accept_language,
                    self.query,
                    ex
                )
                logger.critical(result_print)
                print(result_print)
                self.x_guest_token = self.set_token()
                
            
            ## parsing response 
            try:
                self.tweets = self.response_json['globalObjects']['tweets'].values()
                self.get_tweets(self.tweets)
            except Exception as ex:
                result_print = "lan_type={0:<10}|query={1:<20}|paring error| error={2}|".format(
                    self.accept_language,
                    self.query,
                    ex
                )
                logger.critical(result_print)
                print(result_print)
                continue
            

            
            
    def get_tweets(self,tweets):  
        ## tweets to tweet
        for tweet in tweets:                    
            is_quote_status = tweet['is_quote_status']
            tweet['start_timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            if is_quote_status==False:    
                self.totalcount = self.totalcount + 1
                ## send kafka 
                try:       
                    tweet = json.dumps(tweet, indent=4, sort_keys=True, ensure_ascii=False)
                    self.producer.send("tweet", tweet.encode('utf-8'))
                    self.producer.flush()
                except Exception as ex:
                    logger.critical(ex)
                    print(ex)
                    
        self.refresh_requests_setting()
        
        
    def refresh_requests_setting(self):
        self.cursor = GetCursor.get_refresh_cursor(self.response_json)
        
        result_print = "lan_type={0:<10}|query={1:<20}|tweet_count={2:<10}|".format(
                self.accept_language,
                self.query,
                self.totalcount
        )
        print(result_print)
        logger.critical(result_print)