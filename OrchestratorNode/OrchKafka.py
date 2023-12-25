#!/usr/bin/env python3
from kafka import KafkaProducer,KafkaConsumer
import threading
import json
from streamlit.runtime.scriptrunner import add_script_run_ctx

#consuming topics
consumer_topics = ['register','metrics','heart_beat']

class OrchKafka:
    consumer = None
    producer = None
    consumerThread_id=None

    driverCount = 0

    DriverDetails = {}

    def __init__(self,consumer_topics,register_handler,metrics_handler,heart_beat_handler):
        self.consumer = KafkaConsumer(*consumer_topics,value_deserializer=lambda m: json.loads(m.decode('ascii')))
        self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
        
        self.registerHandler = register_handler
        self.metricsHandler = metrics_handler
        self.heartBeatHandler = heart_beat_handler


    def StartOrch(self):
        # print("STarted")
        self.consumerThread_id = threading.Thread(target=self.ConsumerHandler,args=(self,))
        add_script_run_ctx(self.consumerThread_id)
        self.consumerThread_id.start()
        pass

    def ConsumerHandler(self,ref):
        for record in self.consumer:
            try:
                if record.topic == 'register':
                    self.registerHandler(record.value)
                elif record.topic == 'metrics':
                    self.metricsHandler(record.value)
                elif record.topic == 'heart_beat':
                    self.heartBeatHandler(record.value)
            except Exception as e:
                print("Something happend ",e)
            except :
                print("Something unknown happend ")
                
    def SendMessage(self,topic,object):
        self.producer.send(topic,object)
        self.producer.flush()




if __name__ == "__main__":
    orch = OrchKafka(consumer_topics)
    orch.StartOrch()
    orch.consumerThread_id.join()