import asyncio

async def task1():
    print("Hello")
    await asyncio.sleep(3)
    print("World!")     


async def task2():
    print("Hello again from task2!")
    await asyncio.sleep(1)
    print("World! again from task2!")

async def main():
    await asyncio.gather(task1(), task2())

if __name__ == "__main__":
    asyncio.run(main())         
    

