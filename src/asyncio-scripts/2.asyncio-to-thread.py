import asyncio
import time

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
    tasks = [
        asyncio.to_thread(func1),
        asyncio.to_thread(func2),
        asyncio.to_thread(func3),
    ]

    results = await asyncio.gather(func4(), *tasks)

    return results



print(asyncio.run(main()))


