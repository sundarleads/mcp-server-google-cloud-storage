import asyncio 
global_list = []

async def producer(qName : asyncio.Queue) :
    for i in range(10):
        await qName.put(i)
        print(f"placed the file {i} in {qName}")
        await asyncio.sleep(1)


async def consumer( workerName : str, qName: asyncio.Queue) : 
    while True :
        value = await qName.get()
        global_list.append(f"This is the value : {value}, i have consumed from the queue {qName}, processed by {workerName} \n")
        print(f"This is the value : {value}, i have consumed from the queue {qName}, processed by {workerName} \n")
        qName.task_done()


async def main():

    message_queue = asyncio.Queue(maxsize=20)
    
    await asyncio.create_task(producer(message_queue)),
    workers = [
        asyncio.create_task(consumer("W-1", message_queue)),
        asyncio.create_task(consumer("W-2", message_queue)),
        asyncio.create_task(consumer("W-3", message_queue))
    ]

    await message_queue.join()

    for w in workers:
        w.cancel()
    

asyncio.run(main())
print(global_list)























