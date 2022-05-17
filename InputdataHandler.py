import os
import time
from itertools import repeat
import socket
import AuthenticationManager
import multiprocessing
# We must import this explicitly, it is not imported by the top-level
# multiprocessing module.
import multiprocessing.pool
import time

from random import randint


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
        command = "python ScrapingEngine.py --query '%s' --process_number '%s'  --x_guest_token '%s' --conn '%s' --addr '%s'"%(query, process_number, x_guest_token, conn, addr)
        print(command)
        os.system(command)
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
    
    process_pool = multiprocessing.Pool(processes = num_of_lang)
    process_pool.starmap(execute, zip(repeat(query), num_of_lang_list, repeat(x_guest_token), repeat(conn), repeat(addr) ))
    process_pool.close()
    process_pool.join()

if __name__ == '__main__':
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
    
    
    process_pool = MyPool(num_of_query)
    process_pool.map(query_execute,(num_of_query_list))
    process_pool.close()
    process_pool.join()


print("-------%s seconds -----"%(time.time()-start))
