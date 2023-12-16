from kafka import KafkaProducer,KafkaConsumer
import threading
import json
import time
from tests import Tests
from comms import Comms
from logStore import LogStore
from database import Database
import asyncio
import uuid
import sys


def generate_random_node_id():
    test_id = str(uuid.uuid4())
    return test_id

#consuming topics
consumer_topics = ['test_config','trigger']

class DriverKafka(Tests,Comms,LogStore,Database):
    consumerThread_id=None
    heartId = None
    
    
    def __init__(self,consumer_topics,node_id,master_node_ip,driver_node_ip,beatInterval):
        Database.__init__(self)
        self.node_id = node_id
        self.node_ip = driver_node_ip
        self.master_node_ip=master_node_ip
        self.consumer_topics=consumer_topics
        self.heartInterval = beatInterval
        Tests.__init__(self)
        Comms.__init__(self)
        LogStore.__init__(self)
        
        

    def StartDriver(self):
        print("Starting Driver")
        self.consumerThread_id = threading.Thread(target=self.ConsumerHandler,args=(self,))
        self.consumerThread_id.start()
        self.producer.send('register',{
            'node_id':self.node_id,
            'node_ip':self.node_ip
        })
        self.producer.flush()
        self.heartId = threading.Thread(target=self.HeartBeat,args=(self.heartInterval,))
        self.heartId.start()
        

    def ConsumerHandler(self,ref):
        
            for record in self.consumer:
                try:
                    if record.topic == 'trigger':
                        self.HandleTrigger(record.value)
                    elif record.topic == 'test_config':
                        self.HandleTestConfig(record.value)
                except Exception as e:
                    print("Something happend ",e)
                except :
                    print("Something unknown happend ")



    def HandleTestConfig(self,object):
        
        self.InsertTestData(object)

    
    def HandleTrigger(self,object):
        testConfig=self.getTestConfig(object["test_id"])
        print(testConfig)
        if testConfig[1]=="Tsunami":
            asyncio.run(self.Tsunami(testID=testConfig[0],url=testConfig[3],num_requests=testConfig[2],delay=testConfig[4],node_id=self.node_id))
        elif testConfig[1]=="Avalanche":
            asyncio.run(self.Avalanche(test_id=testConfig[0],url=testConfig[3],num_request=testConfig[2],node_id=self.node_id))
    
    def HeartBeat(self,heartBeatInterVal):

        
        message = {"node_ip":self.node_ip,'node_id':self.node_id,'status':'alive'}
        print("Sent",flush=True)
        while True:
            try:
                time.sleep(heartBeatInterVal)
                self.producer.send('heart_beat',message)
                self.producer.flush()
            except Exception as e:
                print("Something happend ",e)
            except :
                print("Something unknown happend ")

if __name__ == "__main__":
    Tsunami={"test_id":1,"target":"http://localhost:3010/","type":"tsunami","params":{"delay":1,"number_of_request":10,"header":""}}
    Avalanche={"test_id":2,"target":"http://localhost:3010/","type":"avalanche","number_of_request":10,"params":{"header":"","delay":0}}

    master_node_ip=sys.argv[1]
    driver_node_ip=sys.argv[2]
    heartBeat=int(sys.argv[3])
    
    driver = DriverKafka(consumer_topics,generate_random_node_id(),master_node_ip,driver_node_ip,heartBeat)
    
    driver.StartDriver()
    
    # driver.HandleTestConfig(Avalanche)
    # driver.HandleTrigger(Avalanche)
    driver.consumerThread_id.join()

    
