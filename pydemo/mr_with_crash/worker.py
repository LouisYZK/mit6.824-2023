"""
MIT 6.824 Lab1 MapReduce
Python Version with crash
worker
"""
import re
import sys
import os
import json
import time
import asyncio
import rpyc
from functools import wraps
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from itertools import groupby
from threading import Lock
import aiofiles
import function
from globals import *

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
rpyc.core.protocol.DEFAULT_CONFIG['allow_all_attrs'] = True
conn = rpyc.connect("localhost", 18861)

executor = ProcessPoolExecutor(max_workers=4)
ioloop = asyncio.get_event_loop()



class Worker:
    def __init__(self):
        pass

    async def do_map_task(self, task: Task):
        await function.map_func(task.filename, task.task_id)

    async def do_reduce_task(self, task: Task):
        t = function.reduce_func
        await ioloop.run_in_executor(executor, t, task.task_id)

    def regist(self):
        id = conn.root.regist_worker()
        self.id = id

    async def do_task(self, task: Task):
        """
        crash:  with some probability task thread exit without
                finshing task, which status will always be in running.
        """
        if crash(task):
            return 

        if task.phase == MAP_TASK:
            await self.do_map_task(task)
        elif task.phase == REDUCE_TASK:
            await self.do_reduce_task(task)
        else:
            raise ValueError("No such task type!...")
        conn.root.update_task(task.task_id, TaskStatusFinish)

    async def run(self):
        while True:
            try:
                task = conn.root.get_task(self.id)
            except:
                break
            if task is None:
                break
            # asyncio.create_task(self.do_task(task))
            await self.do_task(task)
            
            time.sleep(0.5)

        


if __name__ == "__main__":
    worker = Worker()
    worker.regist()
    ioloop.run_until_complete(worker.run())
    print(f'worker {worker.id} exit...')