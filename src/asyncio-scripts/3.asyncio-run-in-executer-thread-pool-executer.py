import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

def func1():
    print("func1")
    time.sleep(3)
    print("func1 completed")
    return "func1"

def func2():
    print("func2")
    time.sleep(3)
    print("func2 completed")
    return "func2"

def func3():
    print("func3")
    time.sleep(3)
    print("func3 completed")
    return "func3"

async def func4():
    print("Async Func4")
    await asyncio.sleep(3)
    print("Async Func4")
    return "func4"


async def main():

    loop = asyncio.get_running_loop()

    with ThreadPoolExecutor(max_workers=3) as executor :
        tasks = [
        loop.run_in_executor(executor, func1),
        loop.run_in_executor(executor, func2),
        loop.run_in_executor(executor, func3),
    ]
        results = await asyncio.gather(func4(), *tasks)
        return results 


print(asyncio.run(main()))


    





