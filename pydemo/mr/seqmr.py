"""
Lab-1 MapReduce Sequencial Version in Python
"""
import os
from itertools import groupby


# Map process
words_list  = []
for file in os.listdir("."):
    if file.endswith("txt"):
        with open(file) as f:
            words = list()
            for line in f:
                for word in line.split(' '):
                    word = ''.join(list(filter(str.isalpha, word)))
                    if word: words.append((word, 1))
        words_list += words

words_list = sorted(words_list, key=lambda x: x[0])

with open("mr-seq", 'a') as fp:
    for word, values in groupby(words_list, lambda x: x[0]):
        count = len(list(values))
        fp.write(f"{word} {count}\n")
    