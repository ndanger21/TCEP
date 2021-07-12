import copy
import datetime as dt
import os
import sys
from collections import OrderedDict

import numpy as np
import pandas as pd

# Author: Benedikt Lins
DATA_PATH = "/home/niels/Downloads/LinearGenerator/LinearGenerator/src/linearroad_data"
SAVE_PATH = "/home/niels/Downloads/LinearGenerator/LinearGenerator/src/linearroad_data_preprocessed.csv"
TRACES_SAVE_PATH = "/home/niels/Downloads/LinearGenerator/LinearGenerator/src/traces/"
#SECTIONS = [i for i in range(45,57)]
#SECTIONS = [i for i in range(47,53)]
SECTIONS = [i for i in range(50,53)]
NUM_PUBLISHERS = int(sys.argv[1]) if len(sys.argv) > 1 else int(6)
STOP_AT_START = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else False
TIMESTEPS = int(sys.argv[3]) if len(sys.argv) > 3 else int(1)
SIM_TIME = int(sys.argv[4]) if len(sys.argv) > 4 else int(900)

#assert len(SECTIONS) == 12
#assert len(SECTIONS) == 6
assert len(SECTIONS) == 3

def load_data():
    data = None
    for df in pd.read_csv(DATA_PATH, header=None, chunksize=200000):
        df.columns = ["Type","Time","VID","Speed","XWay","Lane","Dir","Seg","Pos","QID","S_Init","S_End","DOW","TOD","Day"]
        df = df[(df.Type==0)&(df.Dir==0)].reset_index(drop=True)
        df = df.drop(columns=["Type","Dir","XWay","Lane","Pos","QID","S_Init","S_End","DOW","TOD","Day"])
        if data is None:
            data = df
        else:
            data = pd.concat((data,df))
        print("DATASIZE: {}".format(data.shape))
    return data

def get_ids_passing_sections(data):
    #tmp = data[(data.Seg==SECTIONS[0])|(data.Seg==SECTIONS[1])|(data.Seg==SECTIONS[2])|(data.Seg==SECTIONS[3])|(data.Seg==SECTIONS[4])|(data.Seg==SECTIONS[5])]
    tmp = data[(data.Seg==SECTIONS[0])|(data.Seg==SECTIONS[1])|(data.Seg==SECTIONS[2])]
    #tmp = data[(data.Seg==SECTIONS[0])|(data.Seg==SECTIONS[1])|(data.Seg==SECTIONS[2])|(data.Seg==SECTIONS[3])|(data.Seg==SECTIONS[4])|(data.Seg==SECTIONS[5])|(data.Seg==SECTIONS[6])|(data.Seg==SECTIONS[7])|(data.Seg==SECTIONS[8])|(data.Seg==SECTIONS[9])]
    return tmp.VID.unique()

def get_norm_start_time(times):
    t = times[0]
    if float((t%TIMESTEPS))/TIMESTEPS >= 0.5:
        return (int(t/TIMESTEPS)+1)*TIMESTEPS
    else:
        return int(t/TIMESTEPS)*TIMESTEPS

def compute_traces(data, vids):
    times, VIDs, speeds, sections = [], [], [], []
    done = 0.0
    todo = vids.shape[0]
    for vid in vids:
        tmp_times = []
        tmp = data[data.VID==vid][["Time","Speed","Seg"]].reset_index(drop=True)
        for row in tmp.values:
            if row[2] in SECTIONS:
                if len(tmp_times) > 0:
                    diff = row[0]-tmp_times[-1]
                    if diff > 30:
                        tmp_times.append(tmp_times[-1])
                        VIDs.append(vid)
                        speeds.append(0)
                        sections.append(sections[-1])
                tmp_times.append(row[0])
                VIDs.append(vid)
                speeds.append(row[1])
                sections.append(row[2])
        tmp_times.append(tmp_times[-1]+TIMESTEPS)
        start_time = get_norm_start_time(tmp_times)
        maxT = len(tmp_times)*TIMESTEPS
        tmp_times = np.arange(0, maxT, step=TIMESTEPS)
        tmp_times += start_time
        times += list(tmp_times)
        VIDs.append(vid)
        speeds.append(0)
        sections.append(sections[-1])
        done += 1
        if done%np.random.randint(low=200, high=1000) == 0:
            print("{:9.6f}% Trace done at: {}".format(float(done)/todo*100,dt.datetime.now()))
    data = pd.DataFrame()
    data["Time"] = times
    data["VID"] = VIDs
    data["Speed"] = speeds
    data["Section"] = sections
    return data.sort_values(by="Time").reset_index(drop=True)

def compute_densities(data):
    time_positions = {}
    times = data.Time.unique()
    densities = []
    done = 0.0
    todo = len(times)
    for t in times:
        tmp = data[data.Time==t][["VID","Section","Speed"]]
        for row in tmp.values:
            if row[1] not in time_positions:
                time_positions[row[1]] = []
            for key in time_positions:
                if row[0] in time_positions[key]:
                    time_positions[key].remove(row[0])
                    break
            if row[2] != 0:
                time_positions[row[1]].append(row[0])
        for row in tmp.values:
            densities.append(len(time_positions[row[1]]))
        done += 1
        if done%np.random.randint(low=20, high=100) == 0:
            print("{:9.6f}% Density done at: {}".format(float(done)/todo*100, dt.datetime.now()))
    data["Density"] = densities
    return data

def select_vid(data, done, t):
    now = set(trace_data[trace_data.Time==t].VID.unique())
    nex = set(trace_data[trace_data.Time==t+TIMESTEPS].VID.unique())
    nex2 = set(trace_data[trace_data.Time==t+2*TIMESTEPS].VID.unique())
    vids = list(set.intersection(now,nex,nex2))
    if len(vids) == 0:
        return None
    counter = 0
    selection = np.random.choice(vids)
    while (selection in done and counter <= 20):
        selection = np.random.choice(vids)
        counter += 1
    return selection

def compute_individual_traces(data, num_cars=6, stop_at_start=False, desired_time=180):
    traces = OrderedDict()
    current_vid = {}
    for i in range(num_cars):
        traces[i] = []
        current_vid[i] = None
    already_done = []
    times = data.Time.unique()
    todo = desired_time*60
    round_time = data.Time.max()
    rounds = 0
    print("Creating Traces with length {:5} seconds".format(todo))
    abort = False
    while not abort:
        print("Starting Round: {}".format(rounds))
        for t in times:
            for key in current_vid:
                if current_vid[key] is None:
                    current_vid[key] = select_vid(data, already_done, t)
                    already_done.append(current_vid[key])
                tmp = data[(data.VID==current_vid[key])&(data.Time==t)].values.ravel()
                if tmp.shape[0] == 5 and tmp[2] != 0:
                    if rounds > 0:
                        tmp[0] = t+rounds*round_time
                    traces[key].append(list(tmp))
                elif tmp.shape[0] == 5 and tmp[2] == 0:
                    current_vid[key] = None
                if traces[key][-1][0] >= todo:
                    abort = True
            if abort:
                break
            if t%np.random.randint(low=200, high=1000) == 0:
                print("{:9.6f}% of individual Traces done at: {}".format(float(t+(rounds*round_time))/todo*100.0, dt.datetime.now()))
        rounds += 1
    if stop_at_start:
        stop_duration=60
        for key in traces:
            for event in traces[key]:
                event[0] += stop_duration
    for key in traces:
        row = copy.deepcopy(traces[key][0])
        if row[0] != 0:
            row[0] = 0
            traces[key] = [row] + traces[key]
    for key in traces:
        tmp = pd.DataFrame(traces[key], columns=data.columns)
        next_event = []
        rows = tmp.values
        adjust = 0
        for i in range(rows.shape[0]-1):
            rows[i+1,0] -= adjust
            current_row = rows[i,:]
            next_row = rows[i+1,:]
            diff = next_row[0]-current_row[0]
            if current_row[0] > 0 and diff > TIMESTEPS and diff%TIMESTEPS==0:
                a = diff/TIMESTEPS-1
                adjust += a*TIMESTEPS
                rows[i+1,0] -= a*TIMESTEPS
                next_row = rows[i+1,:]
                diff = next_row[0]-current_row[0]
            next_event.append(diff)
        next_event.append(0)
        tmp["Next"] = next_event
        traces[key] = tmp
        assert tmp[tmp.Next > TIMESTEPS].shape[0] <= 1 and tmp[tmp.Next < 0].shape[0] == 0, (tmp[tmp.Next > TIMESTEPS].shape[0],tmp[tmp.Next < 0].shape[0])
        print("{:2} has {:3} individual car ids".format(key, tmp.VID.unique().shape[0]))
    return traces

def write_linear_road_traces(publisher_traces):
    for key in publisher_traces:
        i = key+1
        publisher_traces[key].to_csv("%s/linearRoadT%s_P%s.csv" % (TRACES_SAVE_PATH, TIMESTEPS, i), sep=",", index=False)

if not os.path.exists(SAVE_PATH):
    data = load_data()
    print("DATA LOADED! Size: {}".format(data.shape))
    vids = get_ids_passing_sections(data)
    print("{} Vehicles passing sections {} found.".format(vids.shape,SECTIONS))
    trace_data = compute_traces(data, vids)
    print("VEHICLE TRACES computed!")
    trace_data = compute_densities(trace_data)
    print("DENSITIES COMPUTED! Saving...")
    #trace_data.to_csv(SAVE_PATH, sep=",", index=False)
else:
    print("TRACE_DATA loaded!")
    trace_data = pd.read_csv(SAVE_PATH)


print("COMPUTING INDIVIDUAL TRACES FOR {:2} PUBLISHERS with STOP_AT_START: {}".format(NUM_PUBLISHERS, STOP_AT_START))
traces = compute_individual_traces(trace_data, num_cars=NUM_PUBLISHERS, stop_at_start=STOP_AT_START, desired_time=SIM_TIME)
print("TRACES COMPUTED. Saving ...")
write_linear_road_traces(traces)



