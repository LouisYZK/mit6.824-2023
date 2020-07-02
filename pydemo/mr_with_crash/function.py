import json
from threading import Lock
import hashlib
from collections import defaultdict
from itertools import groupby
import aiofiles
from worker import ioloop, executor
from globals import *

async def get_words(filename: str):
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


async def map_func(filename: str, task_id: int):
    words = await get_words(filename)
    await ioloop.run_in_executor(executor, gen_kvs, words, task_id, REDUCE_NUM)    

def reduce_func(task_id: int):
    reduce_no = task_id
    words_list = []
    for i in range(8):
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
