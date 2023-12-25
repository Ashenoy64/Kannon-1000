import streamlit as st
from OrchKafka import OrchKafka  # Import Orchestrator class
import time  # For simulating delays
import uuid
import pandas as pd
from Database import Database
from streamlit_autorefresh import st_autorefresh
from streamlit.runtime.scriptrunner import add_script_run_ctx
import sys
import os



def generate_random_test_id():
    test_id = str(uuid.uuid4())
    return test_id


def fetch_tests():
    tests = st.session_state.db.GetAllTest()
    return tests


def display_driver():
    _data = st.session_state.db.GetNodeDetail()


    with st.container():
        start,mid,end = st.columns(3)

        with start:
            st.write("Node ID")
                
        with mid:
            st.write("Node Name")

        with end:
            st.write("Last HeartBeat")
    for node in _data:
        if time.time()-int(node[2]) <=10:
            with st.container():
                    start,mid,end = st.columns(3)

                    with start:
                        st.write(f"{node[0]}")
                    
                    with mid:
                        st.write(f'{node[1]}')

                    with end:
                        st.write(time.time()-int(node[2]))



   



# Function to trigger a test configuration
def create_test_button_handler(_type,numberOfRequest,target,delay,header):
    
    if(type and numberOfRequest and target):
        if(type == 'Tsunami' and not delay):
            return st.warning('Tsunami needs delay param')
        
        test_config = {
            'test_id':generate_random_test_id(),
            'type':_type,
            "number_of_request":numberOfRequest,
            "target":target,
            'params':{
                'header':header,
                'delay':delay
            }
        }
        # print(test_config)
        st.session_state.db.InsertTestData(test_config)
        st.session_state.orch.SendMessage('test_config',test_config)
        st.success("Test Created successfully")
    pass


# Function to trigger a test from Streamlit UI
def create_test():
    st.header("Create Test")
    test_type = st.radio("Select Test Type", ("Avalanche", "Tsunami"))
    target_throughput = st.number_input("Number of requests", value=100,step=10)
    target = st.text_input('Target',placeholder='http://www.example.com')
    delay_interval = 1.0
    if test_type == "Tsunami":
        delay_interval = st.number_input("Delay Interval (seconds)", value=1.0)

    headers=st.text_area("Headers (optional)",placeholder="Headers for get Request")

    st.button("Create Test",on_click=create_test_button_handler,args=(test_type,target_throughput,target,delay_interval,headers))


def TestDisplayHandler(path,test_id=None):
    st.session_state.test_page=path
    st.session_state.test_id = test_id


# Function to trigger a test on the drivers
def trigger_test_on_drivers(test_id):
    trigger_message = {
        "test_id": test_id,
        "trigger": "YES"
    }
    st.session_state.orch.SendMessage('trigger', trigger_message)
    st.session_state.db.UpdateTestStatus('active',test_id)
    st.session_state.test_id = test_id
    st.session_state.test_page='real'


# Function to display all tests and interact with them (Frontend logic + Backend interaction)
def show_all_tests():
    tests = fetch_tests()
    page = st.session_state.test_page if 'test_page' in st.session_state else 'all'

    if page == 'all':
        st.header("All Tests")
        if(tests):
            for test in tests:
                with st.container():
                    start, end = st.columns(2)

                    with start:
                        st.write(f"{test[0]}")
                        
                    with end:
                        end_1, end_2 = st.columns(2)
                        with end_1:
                            st.write(test[-1])
                        with end_2:    
                            if test[-1] == 'inactive':
                                st.button("Trigger", key=test[0], on_click=trigger_test_on_drivers, args=(test[0],))
                            elif test[-1] == 'complete':
                                st.button("View", key=test[0], on_click=TestDisplayHandler, args=('report', test[0]))
                            else:
                                st.button("Watch", key=test[0], on_click=TestDisplayHandler, args=('real', test[0]))
                            
    elif page == 'real':
        test_id = st.session_state.test_id 
        if test_id:
            st.header(f"Real time Load Test for test {test_id}")
            
            node_data  = st.session_state.db.GetNodeReport(test_id)
            # print(node_data,"here")
            # print(node_data)
            for node in node_data:
                with st.container():
                    with st.container():
                        start,_,end = st.columns(3)
                        with start:
                            st.write(f"NodeID {node[0]}")
                        with end:
                            st.write(f'NodeIP: {node[1]}')
                    with st.container():
                        minimum,maximum,mean,median = node[2:]
                        stats_data = {
                    "Statistic": ["Minimum", "Maximum", "Mean", "Median"],
                    "Value": [minimum,maximum,mean,median]
                }
                        stats_df = pd.DataFrame(stats_data)
                        st.table(stats_df)
        st.button('Back',on_click=TestDisplayHandler, args=('all',None))
    elif page == 'report':
        test_id = st.session_state.test_id 
        if test_id:
            st.header(f"Complete Report for Test ID: {test_id}")
            try:
                print(st.session_state.db.GetTestReport(test_id))
                report_data = st.session_state.db.GetTestReport(test_id)[0][1:]
                with st.container():
                    stats_data = {
                        "Statistic": ["Minimum", "Maximum", "Mean", "Median"],
                        "Value": [report_data[0], report_data[1], report_data[2],report_data[3]]
                    }
                    stats_df = pd.DataFrame(stats_data)
                    st.table(stats_df)
            except:
                st.write("No Node Report generated")
        st.button('Back',on_click=TestDisplayHandler, args=('all',None))
        



def HandlerRegistration(obj):    
    st.session_state.db.InsertDriverData(obj)
    st.rerun()

def HandlerMetrics(obj):
    
    st.session_state.db.InsertOrUpdateNodeReport(obj['node_id'],
        obj['test_id'],obj['min_latency'],obj['max_latency'],
        obj['mean_latency'],obj['median_latency']
        )


    if(obj['status']=='completed'):
        st.session_state.db.InsertOrUpdateTestReport(obj['test_id'])
        st.session_state.db.UpdateTestStatus('complete',obj['test_id'])
    
    st.rerun()

def HandleHeartBeat(obj):
    st.session_state.db.InsertOrUpdateDriver(obj)
    st.rerun()
            





def main():
    st.title("Distributed Load Testing System")

    count = st_autorefresh(interval=2000, key="fizzbuzzcounter")

    page = st.sidebar.radio("Navigation", ("Define Test", "Tests","Drivers"))

    if page == "Define Test":
        st.session_state.page_state={}
        create_test()
    elif page == "Tests":
        show_all_tests()
    elif page == "Drivers":
        st.session_state.page_state={}
        display_driver()

def clearDB():
    if os.path.exists('dlts.db'):
        os.remove('dlts.db')


if __name__ == "__main__":
    args = sys.argv[1:]
   
    if 'clearDB' in args and "clearDB" not in st.session_state:
        st.session_state.clearDB = True 
        clearDB()
    
        
    if "db" not in st.session_state:
        st.session_state.db = Database('dlts.db')

    if 'page_state' not in st.session_state:
        st.session_state.page_state={}

    if 'reloader' not in st.session_state:
        st.session_state.reloader = 1


    if 'orch' not in st.session_state:
        st.session_state.orch =  OrchKafka(['register', 'metrics', 'heart_beat'],HandlerRegistration,HandlerMetrics,HandleHeartBeat)

        st.session_state.orch.StartOrch()



    main()
    