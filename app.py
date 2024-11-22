import orch as oc
import driver as dr
import threading as th
import random 
import sys
import time

server_url=sys.argv[1]

if __name__=="__main__":
    Consumer,Producer,Admin,connection=oc.init_orch()
    while True:
        numofnodes=int(input("Enter the number of drivers between 2-8:"))
        register_nodes=th.Thread(target=oc.Registering,args=(Consumer,numofnodes,connection))
        register_nodes.start()
        for i in range(0,numofnodes):
            kafka_url="localhost:9092"
            done=1
            node_id=random.randint(10000,999999)
            driver_nodes=th.Thread(target=dr.driver, args=(kafka_url,server_url,done,node_id))
            driver_nodes.start()
        time.sleep(1)
        while True:
            r=oc.orchestrar(Consumer,Producer,Admin,connection,numofnodes)
            if r=="done":
                break
            if r=="exit":
                exit()