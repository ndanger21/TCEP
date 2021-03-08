#!/usr/bin/env python3

# File name: run_datacollection.py
# Author: Niels Danger
# Date created: 18.05.2018
# Description: schedules a series of simulations with different algorithms, each run with increasing link latency on the mininet links

import datetime
import logging
import subprocess
import sys
import time
from apscheduler.schedulers.background import BackgroundScheduler

# simulation settings
mobility = sys.argv[1].lower() == 'true' # publisher mobility simulation {True, False}
topology = sys.argv[2].lower() # mininet network topology to use {Tree}
nPublishers = sys.argv[3]
nPublishersPerSection = sys.argv[4]

duration = 10 # simulation duration in minutes
total_time_per_sim = (duration * 60) + 300 # total time for each simulation run; duration for actual simulation, + 5min for setup and cleanup of containernet and akka communication
start_latency = 20 # link latency to use in the first simulation run; increased in steps of 10ms
runs_per_algo = 4 # number of runs with increasing latency (start_latency, start_latency + 10, ...) (see below)
time_between_algorithms = runs_per_algo * total_time_per_sim

sched = BackgroundScheduler()
logging.basicConfig()
logging.getLogger('sched').setLevel(logging.INFO)
sched.start()
print("starting data collection runs, this will take time...")
params = { "latency" : start_latency, "algorithm" : "Relaxation" }

def startSimulation():

    subprocess.call(["./run_mininet_simulation.sh", str(duration), params["algorithm"], str(params["latency"]), str(mobility), str(nPublishers), str(nPublishersPerSection)])
    print(str(datetime.datetime.now())+": running "+str(params["algorithm"])+" simulation with latency "+str(params["latency"]))
    params["latency"] = params["latency"] + 10

sched.add_job(startSimulation, 'interval', seconds=total_time_per_sim, next_run_time=datetime.datetime.now(), coalesce=False)


time.sleep(time_between_algorithms - 5)
params["algorithm"] = "MDCEP"
params["latency"] = start_latency
time.sleep(5)
time.sleep(time_between_algorithms - 5)
params["algorithm"] = "MobilityTolerant"
params["latency"] = start_latency
time.sleep(5)
time.sleep(time_between_algorithms - 5)
params["algorithm"] = "Random"
params["latency"] = start_latency
time.sleep(5)
time.sleep(time_between_algorithms - 5)
params["algorithm"] = "GlobalOptimalBDP"
params["latency"] = start_latency
time.sleep(5)
time.sleep(time_between_algorithms - 5)
params["algorithm"] = "Rizou"
params["latency"] = start_latency
time.sleep(time_between_algorithms)
print("end")
sched.shutdown()
