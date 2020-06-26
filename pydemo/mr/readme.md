# MapReduce Python Implement version

```
sh test.sh
```

results like:

```
MapReduece Task over....
*** Starting test.
--- wc test: PASS
Time taken to execute commands is 11 seconds.
```

- `master.py`: dispath map and reduce tasks and monitor whole tasks completion.
- `worker.py`: execute map and reduce tasks and communicate with the master reporting the tasks' status.

## sync
In terms of the synchronization between master and worker, i use the primitives `Lock`, `Queue` in python, which control the tasks' status and consistence of sync data.

## rpc
`rpyc` in python supports the rpc service.

## Intermedients
Though the intermedient kvs from map tasks should be in memeory cache firstly and storing them to disk **periodically**, which charged in paper, this demo store them directlly after the map tasks of each worker, and named as `mr-X-Y`, X is the map task id and Y is the reduce id, which deribed by some partition methods. 

The partition methods mentioned above is `md5 hashed` of keys in the demo.


## Scalability
In the word count demo, adding the number of workers would not decrease the executing time linearly because the reduce work here is too inexpensive.


If you find any problems and bugs, please issue me directly, thanks for that so such!! 