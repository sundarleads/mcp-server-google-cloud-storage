import asyncio
import time

def func1():
    print("func1")
    time.sleep(3)
    print("func1 completed")

def func2():
    print("func2")
    time.sleep(3)
    print("func2 completed")

def func3():
    print("func3")
    time.sleep(3)
    print("func3 completed")

async def func4():
    print("Async Func4")
    await asyncio.sleep(3)
    print("Async Func4")

async def main():

    loop = asyncio.get_running_loop()

    tasks = [
        loop.run_in_executor(None,func1),
        loop.run_in_executor(None,func2),
        loop.run_in_executor(None,func3)
    ]

    await asyncio.gather(func4(), *tasks)

asyncio.run(main())

