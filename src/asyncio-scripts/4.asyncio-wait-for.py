import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

def func(numb : int = 0):
    print(f"func{numb}")
    time.sleep(5)
    return f"func{numb} completed"

async def asfunc(numb : int = 0):
    print(f"func{numb}")
    await asyncio.sleep(2)
    print(f"func{numb} completed")

# async def waitfor():
#     try :

#         results = await asyncio.gather(asfunc(2), 
#                              asyncio.wait_for(asyncio.to_thread(func, 5022), timeout=5))

#         ret = results[1]

#     except asyncio.TimeoutError as e :
#         ret = f"func Not Completed {e}"

#     return ret

# print(asyncio.run(waitfor()))



async def waitfor():
    try :
        l = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=2) as executor : 
            results = await asyncio.gather(asfunc(2), 
                            asyncio.wait_for(l.run_in_executor(executor, func, 36 ), timeout=3))            

        ret = results[1]

    except asyncio.TimeoutError as e :
        ret = f"func Not Completed {e}"

    return ret

print(asyncio.run(waitfor()))




