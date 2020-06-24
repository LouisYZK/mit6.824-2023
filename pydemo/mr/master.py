import os
import sys
import queue
import time
import json
from typing import Optional
from itertools import groupby
from multiprocessing import Queue
import threading
from threading import Lock
from rpyc.utils.server import ThreadedServer
import rpyc

tasks: Queue = Queue()
tasks_to_complete = dict()
for file in os.listdir('.'):
    if file.endswith(".txt"): tasks.put(file); tasks_to_complete[file] = False
MAP_TASKS = tasks
MAP_TASKS_TO_COM = tasks_to_complete

global TASK_ID, REDUCE_TASK_START, REDUCE_TASK_READY
TASK_ID = 0
REDUCE_TASK_START = False
REDUCE_TASK_READY = False
REDUCE_TASK_TO_COM = dict()

REDUCE_TASKS = Queue()


def prepare_reduce_task():
    lock = Lock()
    lock.acquire()
    global REDUCE_TASK_START, REDUCE_TASK_READY
    if not REDUCE_TASK_START:
        REDUCE_TASK_START = True
        lock.release()
        keys_list = list()
        for file in os.listdir('.'):
            if file.endswith('json'):
                with open(file, 'r') as f:
                    words = json.load(f)
                    keys_list += [item[0] for item in words]
        keys_list = sorted(keys_list)

        for word, values in groupby(keys_list):
            with open(f"r-work-{word}.json", 'w') as fp:
                json.dump(list(values), fp)
            REDUCE_TASKS.put(word)
            REDUCE_TASK_TO_COM[word] = True

        REDUCE_TASK_READY = True
        return
    else:
        return 


class MasterService(rpyc.Service):

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
    
    def exposed_complete_reduce_tasks(self, word):
        global REDUCE_TASK_TO_COM
        lock = Lock(); lock.acquire()
        REDUCE_TASK_TO_COM.pop(word)
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
            global MAP_TASKS_TO_COM
            if not MAP_TASKS_TO_COM: ## when equals to {}
                prepare_reduce_task() 
            return None, 0
            lock.release()

        
    def exposed_get_reduce_task(self):
        lock = Lock()
        lock.acquire()
        global REDUCE_TASK_READY
        if not REDUCE_TASKS.empty():
            return REDUCE_TASKS.get(), False
            lock.release()
        else:
            if not REDUCE_TASK_READY:
                return None, False
                lock.release()
            else:
                return None, True
                lock.release()

def done(server: ThreadedServer):
    """check the if main server is done"""
    while True:
        if not REDUCE_TASK_TO_COM and REDUCE_TASK_READY:
            break
        else: time.sleep(3)
    print("MapReduece Task over....")
    server.close()


if __name__ == "__main__":
    t = ThreadedServer(MasterService, port=18861)
    threading.Thread(target=done, args=(t, )).start()

    t.start()