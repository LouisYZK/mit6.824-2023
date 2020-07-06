from time import sleep
import asyncio
from asyncio import get_event_loop, sleep as asleep, gather, ensure_future
from concurrent.futures import ThreadPoolExecutor, wait, Future, ProcessPoolExecutor
from functools import wraps


executor = ProcessPoolExecutor(max_workers=10)
ioloop = get_event_loop()


def nonblocking(func) -> Future:
    @wraps(func)
    def wrapper(*args):
        return ioloop.run_in_executor(executor, func, *args)
    return wrapper


# @nonblocking  # 用线程池封装没法协程化的普通阻塞程序
def foo(n: int):
    """假装我是个很耗时的阻塞调用"""
    print('start blocking task...')
    # sleep(3)
    for i in range(10000): 
        n += i
    print('end blocking task')
    print(n)
    return n


async def coroutine_demo(n: int):
    """我就是个普通的协程"""

    # 协程内不能出现任何的阻塞调用，所谓一朝协程，永世协程
    # 那我偏要调一个普通的阻塞函数怎么办？
    # 最简单的办法，套一个线程池…
    f = ioloop.run_in_executor(executor, foo, n)
    res = await f
    print('xxxx', res)


async def coroutine_demo_2():
    print('start coroutine task...')
    await asleep(1)
    print('end coroutine task')


async def coroutine_main():
    """一般我们会写一个 coroutine 的 main 函数，专门负责管理协程"""
    await gather(
        coroutine_demo(1),
        coroutine_demo_2()
    )


# def main():
#     # ioloop.run_until_complete(coroutine_main())
#     ioloop.run_in_executor()
#     print('all done')

async def main():
    tasks = []
    for i in range(5): 
        # tasks.append(coroutine_demo(i))
        asyncio.create_task(coroutine_demo(i))
    # await gather(*tasks)

# ioloop.run_until_complete(main())

from multiprocessing import Queue
from threading import Thread
import threading
con = threading.Condition()
class FooBar:
    def __init__(self, n):
        self.n = n

    def foo(self) -> None:
        con.acquire()
        # for i in range(self.n):
            
        #     # printFoo() outputs "foo". Do not change or remove this line.
        #     print('foo')
        #     con.notify()
        #     con.wait()
        print('hi!')
        con.notify()
        con.wait()
        print('end!')
        con.release()


    def bar(self) -> None:
        con.acquire()
            # for i in range(self.n):
            #     # printBar() outputs "bar". Do not change or remove this line.
            #     con.wait()
            #     print('bar')
            #     con.notify()
        con.wait()
        print('i see...')
        con.notify()
        con.release()

import time
def foo() -> None:
    con.acquire()
    print(con)
    # for i in range(self.n):
        
    #     # printFoo() outputs "foo". Do not change or remove this line.
    #     print('foo')
    #     con.notify()
    #     con.wait()
    print('hi!')
    con.notify()
    con.wait()
    print('end!')
    con.notify()
    con.release()


def bar() -> None:
    con.acquire()
    print(con)
        # for i in range(self.n):
        #     # printBar() outputs "bar". Do not change or remove this line.
        #     con.wait()
        #     print('bar')
        #     con.notify()
    con.wait()
    print('i see...')
    con.notify()
    con.wait()
    print('i see...')
    con.notify()
    con.release()

# fb = FooBar(2)
# t1 = Thread(target=foo)
# t2 = Thread(target=bar)
# t1.start()
# t2.start()
# t1.join()
# t2.join()

import threading
 

from multiprocessing import Queue
q = Queue()
qe = Queue()
qo = Queue()
class ZeroEvenOdd:
    def __init__(self, n):
        self.n = n
        q.put(0)
        
	# printNumber(x) outputs "x", where x is an integer.
    def zero(self, printNumber: 'Callable[[int], None]') -> None:
        i = 0
        while True:
            num = q.get()
            printNumber(num)
            i += 1
            if i % 2 == 0:
                qe.put(i)
            else:
                qo.put(i)
            if i == self.n:
                qe.put(0)
                qo.put(0)
                break
        
    def even(self, printNumber: 'Callable[[int], None]') -> None:
        while True:
            num = qe.get()
            if not num: break
            printNumber(num)
            q.put(0)
        
    def odd(self, printNumber: 'Callable[[int], None]') -> None:
        while True:
            num = qo.get()
            if not num: break
            printNumber(num)
            q.put(0)
 
if __name__ == "__main__":
 
    def printNumber(n):
        print(n)
    
    so = ZeroEvenOdd(5)
    t1 = Thread(target=so.zero, args=(printNumber,))
    t2 = Thread(target=so.odd, args=(printNumber,))
    t3 = Thread(target=so.even, args=(printNumber,))
    t2.start(); t3.start(); t1.start()
