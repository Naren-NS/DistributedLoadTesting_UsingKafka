# This is the Driver 
#kafka_url,server_url = sys.argv[1:]
import requests
import json
import time
import random
import sys
import statistics as stats
from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading
'''
#1 setup kafka prod to send : test config and trigger
#2 setup kafka cons to get : heartbeat , reg, metrics
'''
def kafka_setup():
	Producer = KafkaProducer(bootstrap_servers="localhost:9092")
	Consumer = KafkaConsumer() 
	Consumer.subscribe(["test_config","trigger"])
	return Consumer,Producer
	
def Register(node_id):
	reg_mesg = {
 	 "node_id": "",
 	 "node_IP": "",
  	 "message_type": "DRIVER_NODE_REGISTER"
	}
	reg_mesg["node_id"] = str(node_id)
	reg_mesg["node_IP"] = ""
	reg_mesg = str(reg_mesg)
	return reg_mesg

def get_metrics(raw,trigger,test_config,node_id):
	metrics = {
        "node_id": "<RANDOMLY GENERATED UNQUE TEST ID>",
        "test_id": "<TEST ID>",
        "report_id": "<RANDOMLY GENERATED ID FOR EACH METRICS MESSAGE>",
        "metrics": {
            "mean_latency": "",
            "median_latency": "",
            "min_latency": "",
            "max_latency": ""
        }
    }
	metrics["report_id"] = str(random.randint(10000,999999))
	metrics["node_id"] = str(node_id)
	metrics["test_id"] = str(trigger["test_id"])
	metrics["metrics"]["mean_latency"] = str(round(stats.mean(raw),4))
	metrics["metrics"]["median_latency"] = str(round(stats.median(raw),4))
	metrics["metrics"]["min_latency"] = str(round(min(raw),4))
	metrics["metrics"]["max_latency"] = str(round(max(raw),4))
	return metrics

def store_metrics(metrics):
	''' given a json and has to save this json in a SQl db '''
	pass

def dhukdhuk(Producer,stop_event,node_id):
	heartbeat = {
  					"node_id": "",
  					"heartbeat": "YES"
				}
	heartbeat["node_id"] = str(node_id)
	hb = json.dumps(heartbeat)
	while not stop_event.is_set():
		time.sleep(2)
		Producer.send("heartbeat",hb.encode("utf-8"))

def driver(kafka_url,server_url,done,node_id):
	Consumer,Producer = kafka_setup()
	reg_mesg = Register(node_id)
	reg_mesg_str = json.dumps(reg_mesg)
	Producer.send("register", reg_mesg_str.encode("utf-8"))
	stop_event = threading.Event()
	thread1 = threading.Thread(target = dhukdhuk, args = (Producer,stop_event,node_id)) 
	while True:
		for message in Consumer:
			msg = message.value.decode('utf8').replace("'", '"')
			if message.topic == "test_config":
				test_config = json.loads(msg)
				for message in Consumer:
					msg = message.value.decode('utf8').replace("'", '"')
					if message.topic == "trigger":
						thread1.start()
						trigger = json.loads(msg)
						raw = []
						for i in range(int(test_config["throughput_per_driver"])):
							time.sleep(float(test_config["test_message_delay"]))
							start_time = time.time()
							response = requests.get(server_url)
							end_time = time.time()
							response_time_seconds = end_time - start_time
							raw.append(round(response_time_seconds*1000,3))
						metrics = get_metrics(raw,trigger,test_config,node_id)
						store_metrics(metrics)
						metrics_str = json.dumps(metrics)
						stop_event.set()
						Producer.send("metrics",metrics_str.encode("utf-8"))
						sys.exit()
					         