from time import sleep
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
    sleep(3)
    print('end blocking task')
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
    for i in range(5): tasks.append(coroutine_demo(i))
    await gather(*tasks)

ioloop.run_until_complete(main())