# Kannon-1000: Distributed Load Testing System

Kannon-1000 is a distributed load testing system designed for scalability and real-time monitoring. It allows users to perform tsunami and avalanche tests, view test statuses, and manage test creation through a Streamlit-powered interface.

## Features

- **Streamlit Dashboard**: Utilizes Streamlit for visualizing driver status, test creation, and test management.
- **Test Modes**: Supports both tsunami and avalanche testing methodologies for load testing.
- **Real-time Updates**: Provides real-time updates on test progress and results.
- **Orchestrator Node**: Orchestrates the entire system, including storing data using SQLite3, managing communication via Kafka, and coordinating between nodes.
- **Driver Nodes**: Responsible for executing request/response cycles and transmitting results back to the orchestrator node.
- **Scalability**: Technically scalable to multiple nodes, but limited to 4 to 5 drivers due to the use of SQLite3.

## System Components

### Orchestrator Node

The Orchestrator Node is the core component of the system:

- **Streamlit Interface**: Displays driver status, test creation, and test management through an Streamlit-powered dashboard.
- **Data Storage**: Utilizes SQLite3 for storing test and driver data.
- **Communication**: Manages communication between nodes using Kafka, facilitating data exchange and coordination.
- **Limitations**: Due to SQLite3 constraints, the number of drivers is capped at 4 to 5.

### Driver Nodes

- **Responsibility**: Perform response request cycles for load testing.
- **Result Transmission**: Send test results back to the Orchestrator Node for aggregation and analysis.

## Installation 

### Requirements

- Python 
- Kafka 


### Setup

1. **Clone or Download**:
   Clone the repository or download the ZIP file and extract it.

2. **Project Structure**:
   - `OrchestratorNode/`
   - `Driver/`
   - `HTTPServer/`

3. **Installation Steps**:
   - **Orchestrator Node**:
     ```bash
     cd OrchestratorNode/
     pip install -r requirements.txt
     ```

   - **Driver Node**:
     ```bash
     cd Driver/
     pip install -r requirements.txt
     ```

## Running the Project

### Orchestrator Node

Run the Orchestrator Node:

```bash
cd OrchestratorNode/
streamlit run Frontend.py
```

Access the Streamlit interface at `http://localhost:8501` to manage tests.

### Driver Node

Run the Driver Node:

```bash
cd Driver/
python3 Driver.py <masternode_ip> <node_name> <heartbeat_interval>
```

Replace `<masternode_ip>`, `<node_name>`, and `<heartbeat_interval>` with your specific values.

## Configuration

- **Kafka Configuration**: Modify `kafka_config.yaml` to adjust Kafka settings.
- **Node Scaling**: Due to SQLite3 limitations, consider adjusting the number of drivers within the system's constraints.


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.






