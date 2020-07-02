"""
MIT 6.824 Lab1 MapReduce
Python Version
worker
"""
import re
import sys
import os
import json
import time
import asyncio
import hashlib
import rpyc
from functools import wraps
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from itertools import groupby
from threading import Lock
import aiofiles

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

executor = ProcessPoolExecutor(max_workers=4)
ioloop = asyncio.get_event_loop()

def nonblocking(func):
    @wraps(func)
    def wrapper(*args):
        return ioloop.run_in_executor(executor, func, *args)
    wrapper.__name__ = func.__name__
    return wrapper

async def get_words(filename):
    async with aiofiles.open(f'../{filename}') as file:
        words = list()
        async for line in file:
            for word in line.split(' '):
                word = ''.join(list(filter(str.isalpha, word)))
                if word: words.append((word, 1))
    words = sorted(words, key=lambda x: x[0])
    return words

def gen_kvs(words, map_id, reduce_num):
    key_values = defaultdict(list)
    
    for key, values in groupby(words, lambda x: x[0]):
        values = list(values)
        hashed_no = hashlib.md5(key.encode('utf8')).digest()[0]
        reduce_no = hashed_no % reduce_num
        key_values[reduce_no] += values

    for reduce_no, values in key_values.items():
        with open(f'mr-{map_id}-{reduce_no}.json', 'w') as fp:
            json.dump(values, fp)


async def word_count(filename:str, map_id: int, conn: rpyc.Connection):
    """Map Function
    :filename: In this demo, we donnot seprate M chunks, use
     the number of files as M;
    :map_id: file id
    :conn: rpc connection
    """
    print(f"map task {map_id} start ... ")
    words = await get_words(filename)
    reduce_num = conn.root.reduce_num

    ## run the cpu intensive task in process and
    ## use eventloop to watch
    ## combination of the asyncio eventloop and process
    await ioloop.run_in_executor(executor, gen_kvs, words, map_id, reduce_num)

    conn.root.complete_map_tasks(filename)
    print(f'map task {filename} complete')


def reduce_word(reduce_no: int):
    words_list = []
    for i in range(1, 9):
        try:
            with open(f"mr-{i}-{reduce_no}.json", 'r') as fp:
                words = json.load(fp)
                words_list += words
        except:
            continue
    words_list = sorted(words_list, key=lambda x: x[0])
    lock = Lock(); lock.acquire()
    with open(f"mr-out-{reduce_no}", "a") as fp:
        for key, values in groupby(words_list, lambda x: x[0]):
                fp.write(f"{key} {len(list(values))}\n")
    lock.release()
    print(f"Finish reduce work: {reduce_no}")


conn = rpyc.connect("localhost", 18861)

async def map_task():
    tasks = []
    while True:
        f, map_id = conn.root.get_map_task()
        print(f)
        if f is not None :
            time.sleep(0.5)
            tasks.append(word_count(f, map_id, conn))
        else: break
    await asyncio.gather(*tasks)

ioloop.run_until_complete(map_task())    

while True:
    ready, reduce_no = conn.root.get_reduce_task()
    if not ready:
        time.sleep(1)
        continue
    if reduce_no is not None : 
        reduce_word(reduce_no)
        conn.root.complete_reduce_tasks(reduce_no)
    else: 
        break
sys.exit(0)