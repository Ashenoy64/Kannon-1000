import sqlite3 
import os
import time



class Database:
    def __init__(self,fileName="db"):
        if os.path.exists(fileName):
            self.conn = sqlite3.connect(fileName,check_same_thread=False)
            self.cursor = self.conn.cursor()
            print("Here")
        else:
            self.conn = sqlite3.connect(fileName,check_same_thread=False)
            self.cursor = self.conn.cursor()
            self.CreateTables()



    def CreateTables(self):
        tests = '''
CREATE TABLE Test (
    test_id TEXT PRIMARY KEY,
    type TEXT CHECK (type IN ('Tsunami', 'Avalanche')),
    number_of_requests INTEGER,
    target TEXT,
    delay INTEGER,
    header TEXT,
    status TEXT CHECK (status IN ('completed', 'pending'))
);


'''



        report = '''
CREATE TABLE test_report (
    test_id VARCHAR(255) REFERENCES Test(test_id),
    url VARCHAR(100),
    min_latency INTEGER,
    max_latency INTEGER,
    mean_latency REAL,
    median_latency REAL
);
'''

        


        try:
            print("Creating Tables")
            self.cursor.execute(tests)
            
            self.cursor.execute(report)
            print("Done creating")
            
        except Exception as e:
            print('Failed to create table due to following reason: ',e)
        



    def InsertTestData(self,object):
        insert_query = '''
        INSERT INTO Test (test_id, type, number_of_requests, target, delay, header, status)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    '''

        data_to_insert = (
            object['test_id'],
            object['type'],
            object['number_of_request'],
            object['target'],
            object['params']['delay'],
            object['params']['header'],
            "pending"

            
        )

        try:
            self.cursor.execute(insert_query, data_to_insert)
            self.conn.commit()
            
        except Exception as e:
            print("Failed to insert test data ",e)
            self.conn.rollback()


    def InsertTestReport(self,object):
        insert_query = '''
            INSERT INTO test_report (test_id, min_latency, max_latency, mean_latency, median_latency, url)
            VALUES (?, ?, ?, ?, ?,?);
        '''

        data_to_insert = (
            object["test_id"],
            object["min_latency"],
            object["max_latency"],
            object["mean_latency"],
            object["median_latency"],
            object["url"]
        )

        try:
            self.cursor.execute(insert_query, data_to_insert)
            self.conn.commit()  
        except Exception as e:
            print("Failed to insert test report:", e)
            self.conn.rollback()
        

    def getTestConfig(self,testID):
        try:
            self.cursor.execute("SELECT * from Test where test_id=?;",(testID,))
            result= self.cursor.fetchall()[0]
            return result
        except Exception as e:
            print(e)
            
    def getTestReport(self,testID):
        try:
            self.cursor.execute("SELECT * from test_report where test_id=?;",(testID,))
            result= self.cursor.fetchall()[0]
            return result
        except Exception as e:
            print(e)





if __name__=='__main__':
    Database('dlts.db')