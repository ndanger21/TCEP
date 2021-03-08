#!/usr/bin/env python3
import datetime
import logging
import subprocess
import sys
import time
from apscheduler.schedulers.background import BackgroundScheduler

mobility = sys.argv[1].lower() == 'true'
topology = sys.argv[2].lower()
algorithm = sys.argv[3]
nPublishers = sys.argv[4]
nPublishersPerSection = sys.argv[5]
latency = sys.argv[6]

print("latency: " , latency)
start_latency = 20
duration = 10
runs_per_algo = 5
total_time_per_sim = (duration * 60) + 300
sched = BackgroundScheduler()
logging.basicConfig()
logging.getLogger('sched').setLevel(logging.INFO)
sched.start()
print("starting data collection runs, this will take time...")

if int(latency) > 0:
    params = { "latency" : latency }
    incr = 0
else:
    params = { "latency" : start_latency }
    incr = 10

def startSimulation():

    subprocess.call(["./run_mininet_simulation.sh", str(duration), str(algorithm), str(params["latency"]), str(mobility), str(nPublishers), str(nPublishersPerSection)])
    print(datetime.datetime.now(), ": running ", algorithm, " simulation with latency ", params["latency"])
    params["latency"] = str(int(params["latency"]) + incr)

sched.add_job(startSimulation, 'interval', seconds=total_time_per_sim, next_run_time=datetime.datetime.now(), coalesce=False)

#time.sleep(((runs_per_algo / 3) * total_time_per_sim) - 5)
#params["latency"] = start_latency
#time.sleep(5)
#time.sleep(((runs_per_algo / 3) * total_time_per_sim) - 5)
#params["latency"] = start_latency
#time.sleep(5)
#time.sleep(((runs_per_algo / 3) * total_time_per_sim) - 5)
time.sleep((runs_per_algo * total_time_per_sim))
print("end")
sched.shutdown()
