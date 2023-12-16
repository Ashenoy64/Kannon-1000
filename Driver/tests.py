import aiohttp
import asyncio
import time 
import statistics



class Tests:
    # def __init__(self):
    #     self.trace_config = aiohttp.TraceConfig()
       

    # async def on_request_start(self,session, trace_config_ctx, params):
    #     trace_config_ctx.start = asyncio.get_event_loop().time()

    # async def on_request_end(self,session, trace_config_ctx, params):
    #     elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start    
    #     self.sendKafkaMessage("Request took {}".format(elapsed))

    async def make_get_request(testID:str,reqNo:int,url:str,session:aiohttp.ClientSession,params:dict or None=None,headers:dict or None=None,body:dict or None=None):
        start=time.monotonic()
        async with session.get(url,params=params,json=body,headers=headers) as response:
        
            result=await response.text()
            latency=time.monotonic()-start
            return [reqNo,latency]



    async def limitSockets(self,reqNo,url,session,sem,delay=0):
        async with sem:
            await asyncio.sleep(delay)
            result=await self.make_get_request(reqNo=reqNo,url=url,session=session)

            return result


    async def Avalanche(self,test_id,node_id:str,url:str,num_request:int=100):
        print("Starting Avalanche")
        connector=aiohttp.TCPConnector(limit=None)
        sem = asyncio.Semaphore(1000)
        latencies=[]
        async with aiohttp.ClientSession(connector=connector) as session:
            
            tasks=[]
            
            for i in range(num_request):
                tasks.append(asyncio.ensure_future(self.limitSockets(reqNo=i,url=url,session=session,sem=sem)))
            for future in asyncio.as_completed(tasks):
                result=await future
                print(result)
                latencies.append(result[-1])
                print("Sending",{"status":"running","node_id":node_id,"test_id":test_id,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies)})
                self.sendKafkaMessage("metrics",{"status":"running","node_id":node_id,"test_id":test_id,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies)})
            
            self.sendKafkaMessage("metrics",{"status":"completed","node_id":node_id,"test_id":test_id,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies)})
        metrics={"status":"completed","node_id":node_id,"test_id":test_id,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies),"url":url}
        # self.storeReport(testID,metrics)
        self.InsertTestReport(metrics)

                
    async def Tsunami(self,testID:str,url:str,node_id:str,num_requests:int=100,delay:int=1):
        print("Starting Tsunami")
        connector=aiohttp.TCPConnector(limit=None)
        sem = asyncio.Semaphore(1)
        latencies=[]
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks=[]
            for i in range(num_requests):
                tasks.append(asyncio.ensure_future(self.limitSockets(reqNo=i,url=url,session=session,sem=sem,delay=delay)))
            for future in asyncio.as_completed(tasks):
                result=await future
                print(result)
                latencies.append(result[-1])
                
                self.sendKafkaMessage("metrics",{"status":"running","node_id":node_id,"test_id":testID,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies)})

            
            self.sendKafkaMessage("metrics",{"status":"completed","node_id":node_id,"test_id":testID,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies)})
        metrics={"status":"completed","node_id":node_id,"test_id":testID,"max_latency":max(latencies),"min_latency":min(latencies),"median_latency":statistics.median(latencies),"mean_latency":statistics.mean(latencies),"url":url}
        # self.storeReport(testID,metrics)
        self.InsertTestReport(metrics)



