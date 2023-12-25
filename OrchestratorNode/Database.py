import sqlite3 
import os
import time
import pandas as pd
import statistics

class Database:
    def __init__(self,fileName):
    
        if os.path.exists(fileName):
            self.conn = sqlite3.connect(fileName,check_same_thread=False)
            self.cursor = self.conn.cursor()
            
        else:
            self.conn = sqlite3.connect(fileName,check_same_thread=False)
            self.cursor = self.conn.cursor()
            self.CreateTables()


    def CreateTables(self):
        tests = '''
CREATE TABLE Test (
    test_id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(50) CHECK (type IN ('Tsunami', 'Avalanche')),
    number_of_requests INT,
    target VARCHAR(255),
    delay INT,
    header TEXT,
    status 
);
'''

        drivers = '''
CREATE TABLE Driver (
    node_id VARCHAR(255) PRIMARY KEY,
    node_ip VARCHAR(255),
    last_heart_beat TIME
);
'''

        report = '''
CREATE TABLE test_report (
    test_id VARCHAR(255) REFERENCES Test(test_id),
    min_latency INTEGER,
    max_latency INTEGER,
    mean_latency REAL,
    median_latency REAL
);
'''

        node_report = '''
CREATE TABLE node_report (
    node_id VARCHAR(255) REFERENCES Driver(node_id),
    test_id VARCHAR(255) REFERENCES Test(test_id),
    min_latency INTEGER,
    max_latency INTEGER,
    mean_latency REAL,
    median_latency REAL
);
'''


        try:
            self.cursor.execute(tests)
            self.cursor.execute(drivers)
            self.cursor.execute(report)
            self.cursor.execute(node_report)
        except Exception as e:
            print('Failed to insert data due to following reason: ',e)
        except:
            print('Failed  to insert data due to unknown reason: ')



    def InsertTestData(self, test_config):
        insert_query = '''
            INSERT INTO Test (test_id, type, number_of_requests, target, delay, header, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''

        data_to_insert = (
            test_config['test_id'],
            test_config['type'],
            test_config['number_of_request'],
            test_config['target'],
            test_config['params']['delay'],
            str(test_config['params']['header']),
            'inactive'
        )

        try:
            self.cursor.execute(insert_query, data_to_insert)
            self.conn.commit()
        except Exception as e:
            print("Failed to insert test data",e)
            self.conn.rollback()


    def InsertDriverData(self, driver_data):
        insert_query = '''
            INSERT INTO Driver (node_id, node_ip, last_heart_beat)
            VALUES (?, ?, ?)
        '''

        data_to_insert = (
            driver_data['node_id'],
            driver_data['node_ip'],
            time.time()
        )

        try:
            self.cursor.execute(insert_query, data_to_insert)
            self.conn.commit()  # Assuming self.conn is your database connection
        except:
            print("Failed to insert driver data")
            self.conn.rollback()

    def UpdateHeartbeat(self, node_id):
        update_query = '''
            UPDATE Driver
            SET last_heart_beat = ?
            WHERE node_id = ?
        '''

        data_to_update = (
            time.time(),
            node_id
        )

        try:
            self.cursor.execute(update_query, data_to_update)
            self.conn.commit()  # Assuming self.conn is your database connection
        except:
            print("Failed to update heartbeat")
            self.conn.rollback()

    def UpdateTestStatus(self, test_id, new_status):
        update_query = '''
            UPDATE Test
            SET status = ?
            WHERE TestID = ?
        '''

        data_to_update = (
            new_status,
            test_id
        )

        try:
            self.cursor.execute(update_query, data_to_update)
            self.conn.commit()  # Assuming self.conn is your database connection
        except:
            print("Failed to update test status")
            self.conn.rollback()
    
    def InsertTestReport(self, test_id, min_latency, max_latency, mean_latency, median_latency):
        insert_query = '''
            INSERT INTO test_report (test_id, min_latency, max_latency, mean_latency, median_latency)
            VALUES (?, ?, ?, ?, ?)
        '''

        data_to_insert = (
            test_id,
            min_latency,
            max_latency,
            mean_latency,
            median_latency
        )

        try:
            self.cursor.execute(insert_query, data_to_insert)
            self.conn.commit()  
        except:
            print("Failed to insert test report:")
            self.conn.rollback()
        

    def InsertOrUpdateNodeReport(self, node_id, test_id, min_latency, max_latency, mean_latency, median_latency):
        select_query = '''
            SELECT COUNT(*) FROM node_report WHERE node_id = ? AND test_id = ?
        '''

        update_query = '''
            UPDATE node_report
            SET min_latency = ?, max_latency = ?, mean_latency = ?, median_latency = ?
            WHERE node_id = ? AND test_id = ?
        '''

        insert_query = '''
            INSERT INTO node_report (node_id, test_id, min_latency, max_latency, mean_latency, median_latency)
            VALUES (?, ?, ?, ?, ?, ?)
        '''

        data_to_check = (node_id, test_id)
        data_to_update = (min_latency, max_latency, mean_latency, median_latency, node_id, test_id)
        data_to_insert = (node_id, test_id, min_latency, max_latency, mean_latency, median_latency)

        try:
            self.cursor.execute(select_query, data_to_check)
            row_count = self.cursor.fetchone()
            # print(row_count,"row_count here")
            if row_count[0]!=0: 
                self.cursor.execute(update_query, data_to_update)
            else: 
                self.cursor.execute(insert_query, data_to_insert)

            self.conn.commit()  
        except Exception as e:
            print("Failed to insert or update node report:",e)
            self.conn.rollback()
        
    def GetAllTest(self):
        query = 'SELECT * FROM Test;'
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except:
            print("Failed to fetch data")


    def GetTestReport(self,test_id):
        query = 'Select * from test_report where test_id = ?'
        try:
            self.cursor.execute(query,(test_id,))
            return self.cursor.fetchall()
        except Exception as e:
            print("Failed to fetch data",e)


    def GetNodeReport(self,test_id):
        query = 'Select DISTINCT node_report.node_id,node_ip,min_latency,max_latency,mean_latency,median_latency from node_report,Driver where node_report.test_id = ? and node_report.node_id = Driver.node_id '
        try:
            self.cursor.execute(query,(test_id,))
            return self.cursor.fetchall()
        except Exception as e:
            print("Failed to fetch data",e)


    def GetNodeDetails(self):
        # conn = sqlite3.connect("DLTS.db")  # Replace with your SQLite database path
        query = "SELECT node_id,node_ip,last_heart_beat FROM Driver ORDER BY last_heart_beat DESC;"  
        df = pd.read_sql_query(query, self.conn)
        return df

    def GetNodeDetail(self):
        query = "SELECT node_id,node_ip,last_heart_beat FROM Driver ORDER BY last_heart_beat DESC;"  
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print("Failed to fetch data",e)


    def InsertOrUpdateDriver(self,obj):
        try:
            obj=dict(obj)
            # print(obj)
            self.cursor.execute('''
                SELECT node_ip FROM Driver WHERE node_id = ?;
            ''', (obj['node_id'],))
            existing_node_ip = self.cursor.fetchone()

            if existing_node_ip:
                # If entry exists, update if IP is different
                if existing_node_ip[0] != obj['node_ip']:
                    self.cursor.execute('''
                        UPDATE Driver SET node_ip = ?, last_heart_beat = ?
                        WHERE node_id = ?;
                    ''', (obj['node_ip'], time.time(), obj['node_id']))
                else:
                    self.cursor.execute('''
                        UPDATE Driver SET  last_heart_beat = ?
                        WHERE node_id = ?;
                    ''', (time.time(), obj['node_id']))
            else:
                self.cursor.execute('''
                    INSERT INTO Driver (node_id, node_ip, last_heart_beat)
                    VALUES (?, ?, ?);
                ''', (obj['node_id'], obj['node_ip'], time.time()))

            self.conn.commit()
        except sqlite3.Error as e:
            print("Failed to insert or update Driver information:", e)
            self.conn.rollback()
        pass

    # def InsertOrUpdateTestReport(self,test_id):
    #     try:
    #         self.cursor.execute('''
    #             SELECT min_latency, max_latency, mean_latency, median_latency
    #             FROM node_report
    #             WHERE test_id = ?;
    #         ''', (test_id,))
    #         node_report_data = self.cursor.fetchall()

    #         if node_report_data:
    #             # Calculate aggregated values
    #             min_latency = min(row[0] for row in node_report_data)
    #             max_latency = max(row[1] for row in node_report_data)
    #             mean_latency = statistics.mean(row[2] for row in node_report_data)
    #             median_latency = statistics.median(row[3] for row in node_report_data)

    #             # Insert or update values in test_report table
    #             self.cursor.execute('''
    #                 INSERT INTO test_report (test_id, min_latency, max_latency, mean_latency, median_latency)
    #                 VALUES (?, ?, ?, ?, ?)
    #                 ON CONFLICT(test_id) DO UPDATE SET 
    #                     min_latency = excluded.min_latency,
    #                     max_latency = excluded.max_latency,
    #                     mean_latency = excluded.mean_latency,
    #                     median_latency = excluded.median_latency;
    #             ''', (test_id, min_latency, max_latency, mean_latency, median_latency))

    #             self.conn.commit()

    #     except Exception as e:
    #         print("Failed to update Test report:", e)
    #         self.conn.rollback()
    def InsertOrUpdateTestReport(self, test_id):
        try:
            self.cursor.execute('''
                SELECT min_latency, max_latency, mean_latency, median_latency
                FROM node_report
                WHERE test_id = ?;
            ''', (test_id,))
            node_report_data = self.cursor.fetchall()

            if node_report_data:
                # Calculate aggregated values
                min_latency = min(row[0] for row in node_report_data)
                max_latency = max(row[1] for row in node_report_data)
                mean_latency = statistics.mean(row[2] for row in node_report_data)
                median_latency = statistics.median(row[3] for row in node_report_data)

                # Check if the test_id exists in test_report
                self.cursor.execute('''
                    SELECT test_id FROM test_report WHERE test_id = ?;
                ''', (test_id,))
                existing_test_id = self.cursor.fetchone()

                if existing_test_id:
                    # Update existing entry
                    self.cursor.execute('''
                        UPDATE test_report
                        SET min_latency = ?,
                            max_latency = ?,
                            mean_latency = ?,
                            median_latency = ?
                        WHERE test_id = ?;
                    ''', (min_latency, max_latency, mean_latency, median_latency, test_id))
                else:
                    # Insert new entry
                    self.cursor.execute('''
                        INSERT INTO test_report (test_id, min_latency, max_latency, mean_latency, median_latency)
                        VALUES (?, ?, ?, ?, ?);
                    ''', (test_id, min_latency, max_latency, mean_latency, median_latency))

                self.conn.commit()

        except sqlite3.Error as e:
            print("Failed to update or insert Test report:", e)
            self.conn.rollback()
    
    def UpdateTestStatus(self,status,test_id):
        query = 'UPDATE Test SET status=? WHERE test_id = ?'
        try:
            self.cursor.execute(query,(status,test_id))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print("Failed to fetch data",e)




# Database('dlts.db').CreateTables()