# from kafka.admin import KafkaAdminClient, NewTopic
# from kafka import KafkaProducer, KafkaConsumer
# from kafka.errors import *
# import random
# import sys
# import json
# import mysql.connector
# from mysql.connector import Error
# from prettytable import PrettyTable
# import threading
# import time

# def maketopics():
#     Admin = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="Orchestrar")
#     print("Creating Topics")
#     topic_names = ['test_config', 'heartbeat', 'register', 'metrics', 'trigger']
#     for topic in topic_names:
#         NewTopic(name=topic, num_partitions=3, replication_factor=3)
#     print("Topics Created")
#     return Admin

# def exitcode(Admin, connection):
#     topic_names = ['test_config', 'heartbeat', 'register', 'metrics', 'trigger']
#     Admin.delete_topics(topics=topic_names)
#     cursor = connection.cursor()
#     if connection.is_connected():
#         cursor.close()
#         connection.close()
#         print("MySQL connection is closed")
#     exit()

# def make_config():
#     test_config_mesg = {
#         "test_id": 0,
#         "test_type": "",
#         "test_message_delay": 0.00,
#         "throughput_per_driver": 0
#     }
#     test_config_mesg["test_id"] = str(random.randint(100000, 999999))
#     tp = int(input("Enter the Desired Throughput Per Driver : "))
#     test_config_mesg["throughput_per_driver"] = str(tp)
#     TOT = input("Select Type of Test: (A) Avalanche / (T) Tsunami : ")
#     if TOT == "A":
#         test_config_mesg["test_type"] = "AVALANCHE"
#     elif TOT == "T":
#         test_config_mesg["test_type"] = "TSUNAMI"
#         delay = input("Enter delay : ")
#         test_config_mesg["test_message_delay"] = delay
#     return test_config_mesg

# def make_trigger():
#     trigger_req = {
#         "test_id": "",
#         "trigger": "YES"
#     }
#     trigger_req["test_id"] = str(random.randint(100000, 999999))
#     return trigger_req

# def kafka_setup():
#     print("Setting Up Kafka Infrastructure")
#     Producer = KafkaProducer(bootstrap_servers="localhost:9092")
#     Consumer = KafkaConsumer()
#     Consumer.subscribe(["register", "heartbeat", "metrics"])
#     return Consumer, Producer

# def Registering(Consumer, n, connection):
#     print("Registering Nodes ...")
#     nodeslist = []
#     i = n
#     print("Start your Driver Nodes ...")
#     for message in Consumer:
#         message = message.value.decode('utf8').replace("'", '"')
#         node = json.loads(message)  # Adjusted JSON decoding without [1:-1]
#         nodeslist.append(node)
#         print("Registered Node : " + node["node_id"])
#         i -= 1
#         if i == 0:
#             break
#     print(f'{n} nodes registered ..')
#     return 

# def setup_db():
#     connection_config_dict = {
#         'user': 'root',
#         'password': 'Naren@123',
#         'host': 'localhost',
#         'database': 'bd_proj'
#     }
#     connection = mysql.connector.connect(**connection_config_dict)
#     return connection

# def insert_metric(data, connection):
#     try:
#         if connection.is_connected():
#             cursor = connection.cursor()
#             create_table_query = """
#             CREATE TABLE IF NOT EXISTS metrics(
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 node_id VARCHAR(255) NOT NULL,
#                 test_id VARCHAR(255) NOT NULL,
#                 report_id VARCHAR(255) NOT NULL,
# 				mean_latency DOUBLE,
# 				median_latency DOUBLE,
# 				min_latency DOUBLE,
# 				max_latency DOUBLE,
# 				timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#             """
#             cursor.execute(create_table_query)
#             connection.commit()
#             insert_metric_query = """
#             INSERT INTO metrics (node_id, test_id, report_id, mean_latency, median_latency, min_latency, max_latency)
# 			VALUES (%s, %s, %s, %s, %s, %s, %s)
#             """
#             metric_data = (data["node_id"], data["test_id"], data["report_id"],
#                            data["metrics"]["mean_latency"], data["metrics"]["median_latency"],
#                            data["metrics"]["min_latency"], data["metrics"]["max_latency"])
#             cursor.execute(insert_metric_query, metric_data)
#             connection.commit()
#             print(f"Inserted metrics data for node {data['node_id']} into the database.")  # Debugging insertion
#     except Error as e:
#         print("Error while connecting to MySQL", e)

# def init_orch():
#     print("Starting ... ")
#     print("Importing Libraries ... ")
#     Admin = maketopics()
#     Consumer, Producer = kafka_setup()
#     connection = setup_db()
#     return Consumer, Producer, Admin, connection

# def showstats(test_id, connection):
#     if connection.is_connected():
#         cursor = connection.cursor()
#         print(f"Executing query for test_id {test_id}")  # Debugging query execution
#         cursor.execute("SELECT * FROM bd_proj.metrics WHERE test_id = %s ", (str(test_id),))
#         result = cursor.fetchall()
#         print(f"test_id : {test_id}")
#         table = PrettyTable()
#         table.field_names = ["node_id", "report_id", "mean_latency", "median_latency", "min_latency", "max_latency"]
#         for i in result:
#             table.add_row([i[1], i[3], i[4], i[5], i[6], i[7]])
#         print(table)

# def start_test_with_duration(Consumer, Producer, connection, n, duration=180):
#     stop_test = threading.Event()  # Flag to signal when to stop the test

#     def send_heartbeats_and_metrics():
#         start_time = time.time()
#         i = 0
#         while not stop_test.is_set() and time.time() - start_time < duration:
#             messages = Consumer.poll(timeout_ms=1000)
#             for tp, message_batch in messages.items():
#                 for message in message_batch:
#                     msg = message.value.decode('utf8').replace("'", '"')
#                     data = json.loads(msg)
#                     if message.topic == "heartbeat":
#                         print(f'heartbeat from {data["node_id"]}')
#                     elif message.topic == "metrics":
#                         print("Received metrics data:", msg)
#                         insert_metric(data, connection)
#                         print(f"Metrics for node {data['node_id']} inserted.")
#                         table = PrettyTable()
#                         table.field_names = ["node_id", "test_id", "report_id", "mean_latency", "median_latency", "min_latency", "max_latency"]
#                         table.add_row([
#                             data["node_id"],
#                             data["test_id"],
#                             data["report_id"],
#                             data["metrics"]["mean_latency"],
#                             data["metrics"]["median_latency"],
#                             data["metrics"]["min_latency"],
#                             data["metrics"]["max_latency"]
#                         ])
#                         print(table)
#                         i += 1
#                         if i == n:
#                             stop_test.set()
#                             break
#             if time.time() - start_time >= duration:
#                 stop_test.set()
#             time.sleep(1)

#     test_thread = threading.Thread(target=send_heartbeats_and_metrics)
#     test_thread.start()
#     test_thread.join(duration)
#     stop_test.set()
#     test_thread.join()
#     print("\nTest completed. You may now select other options.\n")

# def orchestrar(Consumer, Producer, Admin, connection, n):
#     while True:
#         print("Choose Option -")
#         print("[1] : Start Test")
#         print("[2] : Print Metrics")
#         print("[3] : Change Number of Driver Nodes")
#         print("[-1] : Exit")
        
#         option = input("")

#         if option == "1":
#             print("Prepping test_config")
#             test_config_mesg = make_config()
#             test_config_mesg_str = json.dumps(test_config_mesg)
#             Producer.send("test_config", test_config_mesg_str.encode("utf-8"))
            
#             start = input("Trigger Test ? (y/n) : ")
#             if start.lower() == 'y':
#                 print("Triggering Test")
#                 trigger_req = make_trigger()
#                 trigger_req_str = json.dumps(trigger_req)
#                 Producer.send("trigger", trigger_req_str.encode("utf-8"))

#                 # Start test with a fixed duration and wait until completion
#                 start_test_with_duration(Consumer, Producer, connection, n, duration=180)
#             else:
#                 print("Test aborted by user.")
#             continue

#         elif option == "2":
#             test_id = input("Enter test_id : => ")
#             showstats(test_id, connection)
#             continue

#         elif option == "-1":
#             print("Exiting Application .... ")
#             exitcode(Admin, connection)
#             return "exit"

#         elif option == "3":
#             return "done"

#         else:
#             print("Invalid option selected. Please choose a valid option.")
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import *
import random
import sys
import json
import mysql.connector
from mysql.connector import Error
from prettytable import PrettyTable
import threading
import time

def maketopics():
    Admin = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="Orchestrar")
    print("Creating Topics")
    topic_names = ['test_config', 'heartbeat', 'register', 'metrics', 'trigger']
    for topic in topic_names:
        NewTopic(name=topic, num_partitions=3, replication_factor=3)
    print("Topics Created")
    return Admin

def exitcode(Admin, connection):
    topic_names = ['test_config', 'heartbeat', 'register', 'metrics', 'trigger']
    Admin.delete_topics(topics=topic_names)
    cursor = connection.cursor()
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
    exit()

def make_config():
    test_config_mesg = {
        "test_id": 0,
        "test_type": "",
        "test_message_delay": 0.00,
        "throughput_per_driver": 0
    }
    test_config_mesg["test_id"] = str(random.randint(100000, 999999))
    tp = int(input("Enter the Desired Throughput Per Driver : "))
    test_config_mesg["throughput_per_driver"] = str(tp)
    TOT = input("Select Type of Test: (A) Avalanche / (T) Tsunami : ")
    if TOT == "A":
        test_config_mesg["test_type"] = "AVALANCHE"
    elif TOT == "T":
        test_config_mesg["test_type"] = "TSUNAMI"
        delay = input("Enter delay : ")
        test_config_mesg["test_message_delay"] = delay
    return test_config_mesg

def make_trigger():
    trigger_req = {
        "test_id": "",
        "trigger": "YES"
    }
    trigger_req["test_id"] = str(random.randint(100000, 999999))
    return trigger_req

def kafka_setup():
    print("Setting Up Kafka Infrastructure")
    Producer = KafkaProducer(bootstrap_servers="localhost:9092")
    Consumer = KafkaConsumer()
    Consumer.subscribe(["register", "heartbeat", "metrics"])
    return Consumer, Producer

def Registering(Consumer, n, connection):
    print("Registering Nodes ...")
    nodeslist = []
    i = n
    print("Start your Driver Nodes ...")
    for message in Consumer:
        message = message.value.decode('utf8').replace("'", '"')
        node = json.loads(message)  # Adjusted JSON decoding without [1:-1]
        nodeslist.append(node)
        print("Registered Node : " + node["node_id"])
        i -= 1
        if i == 0:
            break
    print(f'{n} nodes registered ..')
    return 

def setup_db():
    connection_config_dict = {
        'user': 'root',
        'password': 'Naren@123',
        'host': 'localhost',
        'database': 'bd_proj'
    }
    connection = mysql.connector.connect(**connection_config_dict)
    return connection

def insert_metric(data, connection):
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS metrics(
                id INT AUTO_INCREMENT PRIMARY KEY,
                node_id VARCHAR(255) NOT NULL,
                test_id VARCHAR(255) NOT NULL,
                report_id VARCHAR(255) NOT NULL,
				mean_latency DOUBLE,
				median_latency DOUBLE,
				min_latency DOUBLE,
				max_latency DOUBLE,
				timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            connection.commit()
            insert_metric_query = """
            INSERT INTO metrics (node_id, test_id, report_id, mean_latency, median_latency, min_latency, max_latency)
			VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            metric_data = (data["node_id"], data["test_id"], data["report_id"],
                           data["metrics"]["mean_latency"], data["metrics"]["median_latency"],
                           data["metrics"]["min_latency"], data["metrics"]["max_latency"])
            cursor.execute(insert_metric_query, metric_data)
            connection.commit()
            print(f"Inserted metrics data for node {data['node_id']} into the database.")  # Debugging insertion
    except Error as e:
        print("Error while connecting to MySQL", e)

def init_orch():
    print("Starting ... ")
    print("Importing Libraries ... ")
    Admin = maketopics()
    Consumer, Producer = kafka_setup()
    connection = setup_db()
    return Consumer, Producer, Admin, connection

def showstats(test_id, connection):
    if connection.is_connected():
        cursor = connection.cursor()
        print(f"Executing query for test_id {test_id}")  # Debugging query execution
        cursor.execute("SELECT * FROM bd_proj.metrics WHERE test_id = %s ", (str(test_id),))
        result = cursor.fetchall()
        print(f"test_id : {test_id}")
        table = PrettyTable()
        table.field_names = ["node_id", "report_id", "mean_latency", "median_latency", "min_latency", "max_latency"]
        for i in result:
            table.add_row([i[1], i[3], i[4], i[5], i[6], i[7]])
        print(table)

def orchestrar(Consumer, Producer, Admin, connection, n):
    while True:
        print("Choose Option -")
        print("[1] : Start Test")
        print("[2] : Print Metrics")
        print("[3] : Change Number of Driver Nodes")
        print("[-1] : Exit")
        
        option = input("")

        if option == "1":
            print("Prepping test_config")
            test_config_mesg = make_config()
            test_config_mesg_str = json.dumps(test_config_mesg)
            Producer.send("test_config", test_config_mesg_str.encode("utf-8"))
            
            start = input("Trigger Test ? (y/n) : ")
            if start.lower() == 'y':
                print("Triggering Test")
                trigger_req = make_trigger()
                trigger_req_str = json.dumps(trigger_req)
                Producer.send("trigger", trigger_req_str.encode("utf-8"))

                print("Receiving heartbeats and metrics...")
                for message in Consumer:
                    msg = message.value.decode('utf8').replace("'", '"')
                    data = json.loads(msg)
                    if message.topic == "heartbeat":
                        print(f'heartbeat from {data["node_id"]}')
                    elif message.topic == "metrics":
                        print("Received metrics data:", msg)
                        insert_metric(data, connection)
                        print(f"Metrics for node {data['node_id']} inserted.")
                        table = PrettyTable()
                        table.field_names = ["node_id", "test_id", "report_id", "mean_latency", "median_latency", "min_latency", "max_latency"]
                        table.add_row([
                            data["node_id"],
                            data["test_id"],
                            data["report_id"],
                            data["metrics"]["mean_latency"],
                            data["metrics"]["median_latency"],
                            data["metrics"]["min_latency"],
                            data["metrics"]["max_latency"]
                        ])
                        print(table)
            
            else:
                print("Test aborted by user.")
            continue

        elif option == "2":
            test_id = input("Enter test_id : => ")
            showstats(test_id, connection)
            continue

        elif option == "-1":
            print("Exiting Application .... ")
            exitcode(Admin, connection)
            return "exit"

        elif option == "3":
            return "done"

        else:
            print("Invalid option selected. Please choose a valid option.")
