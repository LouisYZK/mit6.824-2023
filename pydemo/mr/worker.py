"""
MIT 6.824 Lab1 MapReduce
Python Version
worker
"""
import re
import os
import json
import time
import hashlib
import rpyc
from itertools import groupby
from threading import Lock

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True


def word_count(filename:str, map_id: int, conn: rpyc.Connection):
    """Map Function
    :filename: In this demo, we donnot seprate M chunks, use
     the number of files as M;
    :map_id: file id
    :conn: rpc connection
    """
    with open(filename) as file:
        words = list()
        for line in file:
            for word in line.split(' '):
                word = ''.join(list(filter(str.isalpha, word)))
                if word: words.append((word, 1))
    words = sorted(words, key=lambda x: x[0])
    key_values = dict()
    for key, values in groupby(words, lambda x: x[0]):
        values = list(values)
        hashed_no = hashlib.md5(key.encode('utf8')).digest()[0]
        reduce_no = hashed_no % conn.root.reduce_num
        if reduce_no not in key_values:
            key_values[reduce_no] = values
        else: key_values[reduce_no] += values
    
    for reduce_no, values in key_values.items():
        with open(f'mr-{map_id}-{reduce_no}.json', 'w') as fp:
            json.dump(values, fp)
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


if __name__ == '__main__':
    import sys
    conn = rpyc.connect("localhost", 18861)
    while True:
        f, map_id = conn.root.get_map_task()
        print(f)
        if f is not None :
            word_count(f, map_id, conn)
        else: break
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