"""
MIT 6.824 Lab1 MapReduce
Python Version with Crash Tests
master server
"""
import os
import sys
import queue
import time
import json
from typing import Optional, List
from itertools import groupby
from multiprocessing import Queue, Process
from threading import Lock, Condition, Thread
from rpyc.utils.server import ThreadedServer
import rpyc
from globals import *

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
rpyc.core.protocol.DEFAULT_CONFIG['allow_all_attrs'] = True
lock = Lock()
condition = Condition()

global TASK_STATUS
TASK_STATUS: List[Task] = []
TASK_Q = Queue()
TASK_PHASE = MAP_TASK

DONE = False

global WORKER_ID
WORKER_ID = 0


class MasterService(rpyc.Service):

    def on_connect(self, conn):
        pass

    def on_disconnect(self, conn):
        pass
    
    def exposed_regist_worker(self):
        with lock:
            global WORKER_ID
            WORKER_ID += 1
            return WORKER_ID

    def exposed_update_task(self, task_id: int, status):
        """task_id should be same as seq in list
        """
        with lock:
            TASK_STATUS[task_id].status = status

    def exposed_get_task(self, worker_id: int) -> Task:
        # with condition:
        task = TASK_Q.get()
        with lock:
            TASK_STATUS[task.task_id].status = TaskStatusRunning
            TASK_STATUS[task.task_id].start_time = time.time()
        task.worker_id = worker_id
        print(f'worker {worker_id} acquire task {task.task_id}')
        return task


def init_map_tasks():
    for id, f in enumerate(FileNames):
        t = Task(filename=f, task_id=id, phase=MAP_TASK)
        t.status = TaskStatusReady
        TASK_STATUS.append(t)

def init_reduce_task():
    print('init reduce task...')
    global TASK_STATUS, TASK_PHASE
    TASK_STATUS = []
    for id in range(REDUCE_NUM):
        t = Task(task_id=id, phase=REDUCE_TASK)
        t.status = TaskStatusReady
        TASK_STATUS.append(t)
    TASK_PHASE = REDUCE_TASK

def add_task(task: Task):
    # with condition:
    task.status = TaskStatusQueue
    print(f'{task} are put in queue...')
    TASK_Q.put(task)
        # condition.notify()

def check_heart_break(task: Task):
    time_gap = time.time() - task.start_time
    print(task, 'ttttttttttttt->', time_gap)
    if time_gap > MaxTaskRunTime:
        add_task(task) 


def schedule():
    all_finished = True
    global TASK_STATUS, DONE
    with lock:
        for task in TASK_STATUS:
            if task.status == TaskStatusReady:
                all_finished = False
                add_task(task)
        
            elif task.status == TaskStatusQueue:
                all_finished = False
        
            elif task.status == TaskStatusRunning:
                all_finished = False
                time_gap = time.time() - task.start_time
                print(task, 'ttttttttttttt->', time_gap)
                if time_gap > MaxTaskRunTime:
                    add_task(task) 

            elif task.status == TaskStatusFinish:
                # print(task)
                pass

            elif task.status == TaskStatusErr:
                all_finished = False
                add_task(task)

            else:
                raise ValueError("wrong task status!")

        if all_finished:
            if TASK_PHASE == MAP_TASK:
                init_reduce_task()
                print('Map task finished...')
            else:
                print('Reduce task finished...')
                DONE = True

def task_loop():
    while True:
        schedule()
        time.sleep(ScheduleInterval)
        
def done(server: ThreadedServer):
    """check the if main server is done"""
    while True:
        if not DONE:
            time.sleep(1)
        else:
            TASK_Q.put(None)
            break
    print("MapReduece Task over....")
    server.close()

    

if __name__ == "__main__":
    init_map_tasks()

    t = ThreadedServer(MasterService, port=18861)
    task_sched_thread = Thread(target=task_loop)
    check_done_thread = Thread(target=done, args=(t, ))
    
    task_sched_thread.start()
    check_done_thread.start()
    t.start()
    os._exit(0)