import os
import random

# some consts here
MAP_TASK = 'map'
REDUCE_TASK = 'reduce'

CRASH_PROB = 0.3

REDUCE_NUM = 50

( TaskStatusReady, TaskStatusQueue, 
TaskStatusRunning, TaskStatusFinish,
TaskStatusErr ) = range(5)

MaxTaskRunTime = 3
ScheduleInterval = 0.2

FileNames = []
for f in os.listdir('..'): 
    if f.endswith('.txt'): FileNames.append(f)


class Task:
    def __init__(self, filename=None, reduce_num=REDUCE_NUM,
                 map_num=None, task_id=None, phase=MAP_TASK):
        self.phase = phase
        self.reduce_num = reduce_num
        self.task_id = task_id
        if map_num is None:
            self.map_num = len(FileNames)
        self.filename = filename if phase == MAP_TASK else ''
        
    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, current_status: int):
        if current_status not in range(5):
            raise ValueError("status set error!")
        self._status = current_status

    @property
    def worker_id(self):
        return self._worker_id

    @worker_id.setter
    def worker_id(self, id: int):
        self._worker_id = id

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, time: float):
        self._start_time = time


    def __repr__(self):
        return f'{self.phase} task {self.task_id} are in {self.status}'


def crash(task: Task):
    rand = random.random()
    if rand < CRASH_PROB:
        print(task, '----> crashed !!!!')
        return True
    return False
