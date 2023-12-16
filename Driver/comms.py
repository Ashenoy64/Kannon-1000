from kafka import KafkaProducer,KafkaConsumer
import json
import time
import sqlite3




class Comms:
        
    def __init__(self):
        self.consumer = KafkaConsumer(*self.consumer_topics,value_deserializer=lambda m: json.loads(m.decode('ascii')))
        self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

    
    def sendKafkaMessage(self,topic,object):
        self.producer.send(topic,object)
        self.producer.flush()
    
    

    



