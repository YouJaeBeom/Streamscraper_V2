import argparse 
import requests
import json
import sys
import time
import maya
import socket
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


TCP_IP = "117.17.189.206"
TCP_PORT = 13000
conn = None
# create a socket object
s = socket.socket()
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("listen")

conn, addr = s.accept()
print(conn, addr)



class ScrapingEngine(object):
    def __init__(self, query, process_number, x_guest_token, conn, addr):
        ## Setting connection 
        self.conn = conn
        self.addr = addr

        ## Setting query
        self.query = query
        
        ## Setting process number
        self.process_number = process_number
        
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
        with open('language_list.txt', 'r') as f:
            language_list_txt = f.read().split(",")
        self.language_list =[]
        for language in language_list_txt:
            language=language.strip()
            self.language_list.append(language)   
            
        self.accept_language = self.language_list[int(self.process_number)]
        self.x_twitter_client_language = self.language_list[int(self.process_number)]
        
        
        
        
    def set_search_url(self):
        self.url = self.base_url + self.query +"&src=typed_query&f=live"
        
        return self.url

    def set_token(self):
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
                ## send socket
                try:
                    self.conn.send((json.dumps(tweet)+"\n").encode('utf-8'))
                except Exception as es:
                    logger.critical(es)
                    print("xxxxx",es)
                    continue
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
        
        
        

        
        
class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

# This block of code enables us to call the script from command line.
def execute(query,process_number,x_guest_token, conn, addr):
    try:
        streamscraper = ScrapingEngine(query, process_number, x_guest_token, conn, addr)
        streamscraper.start_scraping()        
        command = "python ScrapingEngine.py --query '%s' --process_number '%s'  --x_guest_token '%s' --conn '%s' --addr '%s'"%(query, process_number, x_guest_token, conn, addr)
        print(command)
        """os.system(command)"""
    except Exception as ex:
        pass

def query_execute(query_index):
    
    ## language_list
    with open('language_list.txt', 'r') as f:
        language_list_txt = f.read().split(",")
    
    language_list =[]
    for language in language_list_txt:
        language=language.strip()
        language_list.append(language)

    num_of_lang = len(language_list)
    num_of_lang_list = []

    for index in range(0,num_of_lang):
        num_of_lang_list.append(index)
        
    ## query list 
    with open('list.txt', 'r') as f:
        query_list_txt = f.read().split(',')

    query_list =[]
    for query in query_list_txt:
        query=query.strip()
        query_list.append(query)

    query = query_list[query_index]

    x_guest_token = None
    while True:
        x_guest_token = AuthenticationManager.get_x_guest_token()
        if x_guest_token != None:
            break
    
    
    
    process_pool = multiprocessing.Pool(processes = num_of_lang)
    process_pool.starmap(execute, zip(repeat(query), num_of_lang_list, repeat(x_guest_token), repeat(conn), repeat(addr) ))
    process_pool.close()
    process_pool.join()
    
    
if(__name__ == '__main__') :
    start=time.time()
    
    ## query list 
    with open('list.txt', 'r') as f:
        query_list_txt = f.read().split(',')

    query_list =[]
    for query in query_list_txt:
        query=query.strip()
        query_list.append(query)

    num_of_query = len(query_list)

    num_of_query_list = []

    for index in range(0,num_of_query):
        num_of_query_list.append(index)
    
    
    print(conn, addr)
    
    process_pool = MyPool(num_of_query)
    process_pool.map(query_execute,(num_of_query_list))
    process_pool.close()
    process_pool.join()
    
    """parser = argparse.ArgumentParser()
    parser.add_argument("--query",help="add query")
    parser.add_argument("--process_number", help="add process_number")
    parser.add_argument("--x_guest_token", help="add init x_guest_token")
    parser.add_argument("--conn", help="add init conn")
    parser.add_argument("--addr", help="add init addr")
    args = parser.parse_args()
    
    streamscraper = ScrapingEngine(args.query, args.process_number, args.x_guest_token, args.conn, args.addr)
    streamscraper.start_scraping()"""

print("-------%s seconds -----"%(time.time()-start))

