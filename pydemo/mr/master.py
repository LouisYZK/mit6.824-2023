"""
MIT 6.824 Lab1 MapReduce
Python Version
master server
"""
import os
import sys
import queue
import time
import json
from typing import Optional
from itertools import groupby
from multiprocessing import Queue, Process
import threading
from threading import Lock
from rpyc.utils.server import ThreadedServer
import rpyc

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

tasks: Queue = Queue()
tasks_to_complete = dict()
for file in os.listdir('..'):
    if file.endswith(".txt"): tasks.put(file); tasks_to_complete[file] = False
MAP_TASKS = tasks
global MAP_TASKS_TO_COM
MAP_TASKS_TO_COM = tasks_to_complete
global MAP_BUFFERED
MAP_BUFFERED = list()


global TASK_ID, REDUCE_TASK_START, REDUCE_TASK_READY
TASK_ID = 0
REDUCE_TASK_START = False


REDUCE_TASK_READY = False
REDUCE_TASK_TO_COM = dict()
REDUCE_TASKS = Queue()
REDUCE_NUM = 50
for i in range(REDUCE_NUM):
    REDUCE_TASKS.put(i); REDUCE_TASK_TO_COM[i] = False


def restore_map_buffer_to_disk():
    """
    `Periodically, the buffered pairs are written to local disk,
     partitioned into R regions by the partitioning function. 
    ` -- from mapreduce paper.
    """
    global REDUCE_TASK_READY, MAP_BUFFERED, REDUCE_TASKS, REDUCE_TASK_TO_COM

    words_list = sorted(MAP_BUFFERED, key=lambda x: x[0])
    key_values = dict()
    for key, values in groupby(words_list, lambda x: x[0]):
        no = hash(key) % REDUCE_NUM
        if no not in key_values: key_values[no] = list(values)
        else: key_values[no].extend(list(values))

    for reduce_no, values in key_values.items():
        reduce_file_name = f'r-{reduce_no}.json'
        with open(reduce_file_name, 'w') as fp:
            json.dump(values, fp)
        REDUCE_TASKS.put(reduce_no)
        REDUCE_TASK_TO_COM[reduce_no] = False

    REDUCE_TASK_READY = True

def partition_func(words, filename):
    global MAP_BUFFERED, MAP_TASKS_TO_COM
    lock = Lock(); lock.acquire()
    MAP_BUFFERED += words
    MAP_TASKS_TO_COM.pop(filename)
    lock.release()
        

class MasterService(rpyc.Service):
    global REDUCE_NUM
    exposed_reduce_num = REDUCE_NUM

    def __init__(self):
        pass

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass
    
    def exposed_complete_map_tasks(self, filename):
        global MAP_TASKS_TO_COM
        lock = Lock(); lock.acquire()
        MAP_TASKS_TO_COM.pop(filename)
        lock.release()
    
    def exposed_complete_reduce_tasks(self, reduce_no):
        global REDUCE_TASK_TO_COM
        lock = Lock(); lock.acquire()
        REDUCE_TASK_TO_COM.pop(reduce_no)
        lock.release()

    def exposed_get_map_task(self) -> Optional[str]:
        lock = Lock()
        lock.acquire()
        if not MAP_TASKS.empty():
            global TASK_ID
            TASK_ID += 1
            return MAP_TASKS.get(), TASK_ID
            lock.release()
        else:
            return None, 0
            lock.release()

    def exposed_map_buffered(self, words, filename):
        threading.Thread(target=partition_func, args=(words, filename)).start()

        
    def exposed_get_reduce_task(self):
        lock = Lock()
        lock.acquire()
        global REDUCE_TASK_READY
        if not REDUCE_TASK_READY:
            return False, None
        if not REDUCE_TASKS.empty():
            return True, REDUCE_TASKS.get()
            lock.release()
        else:
            return True, None
            lock.release()


def done(server: ThreadedServer):
    """check the if main server is done"""
    while True:
        if not REDUCE_TASK_TO_COM:
            break
        else: time.sleep(1)
    print("MapReduece Task over....")
    server.close()

def check_map_done():
    while True:
        if not MAP_TASKS_TO_COM:
            print("MAP over....")
            global REDUCE_TASK_READY
            REDUCE_TASK_READY = True
            break
        else: time.sleep(1)
    


if __name__ == "__main__":
    t = ThreadedServer(MasterService, port=18861)
    threading.Thread(target=done, args=(t, )).start()
    threading.Thread(target=check_map_done).start()
    t.start()
    os._exit(0)