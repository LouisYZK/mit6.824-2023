"""
MIT 6.824 Lab1 MapReduce
Python Version
worker
"""
import re
import os
import json
import time
import rpyc
from itertools import groupby
from threading import Lock

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True


def word_count(filename:str, conn: rpyc.Connection):
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
    conn.root.map_buffered(words, filename)


def reduce_word(reduce_no: int):
    with open(f"r-{reduce_no}.json", 'r') as fp:
        time.sleep(0.01)
        words_list = json.load(fp)
    words_list = sorted(words_list, key=lambda x: x[0])
    for key, values in groupby(words_list, lambda x: x[0]):
        with open(f"mr-out-{reduce_no}", "a") as fp:
            fp.write(f"{key} {len(list(values))}\n")
    print(f"Finish reduce work: {reduce_no}")


if __name__ == '__main__':
    import sys
    conn = rpyc.connect("localhost", 18861)
    while True:
        f = conn.root.get_map_task()
        print(f)
        if f is not None :
            word_count(f, conn)
        else: break
    while True:
        reduce_no, complete = conn.root.get_reduce_task()
        if reduce_no is not None : 
            reduce_word(reduce_no)
            conn.root.complete_reduce_tasks(reduce_no)
        if complete: break
    sys.exit(0)