import os
import queue
import time
import json
from typing import Optional
from multiprocessing import Queue
from threading import Lock
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

        unique_words = set(keys_list)
        cursor = 0
        for ind in range(len(keys_list)):
            if keys_list[ind] == keys_list[cursor]:
                continue
            else:
                word = keys_list[cursor]
                with open(f"r-work-{word}.json", 'w') as fp:
                    json.dump(keys_list[cursor: ind - 1], fp)
                cursor = ind

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
                print("MapReduece Task over....")
                lock.release()
    
    

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MasterService, port=18861)
    t.start()