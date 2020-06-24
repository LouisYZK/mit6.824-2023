import re
import os
import json
import time
import rpyc
from threading import Lock


def word_count(filename:str, task_id: int):
    with open(filename) as file:
        words = list()
        for line in file:
            for word in line.split(' '):
                word = ''.join(list(filter(str.isalpha, word)))
                if word: words.append((word, 1))

    with open(f'map_task_{task_id}.json', 'w') as f:
        json.dump(words, f)


def reduce_word(word: str):
    count = 0
    with open(f"r-work-{word}.json", 'r') as fp:
        time.sleep(0.01)
        words = json.load(fp)
        count = len(words)

    with open(f"mr-out-{word}", "w") as fp:
        fp.write(f"{word} {count}\n")
    print(f"Finish reduce work: {word}")


if __name__ == '__main__':
    import sys
    conn = rpyc.connect("localhost", 18861)
    while True:
        f, task_id = conn.root.get_map_task()
        if f is not None :
            word_count(f, task_id)
            conn.root.complete_map_tasks(f)
        else: break
    while True:
        word, complete = conn.root.get_reduce_task()
        if word is not None : 
            reduce_word(word)
            conn.root.complete_reduce_tasks(word)
        if complete: break
    sys.exit(0)