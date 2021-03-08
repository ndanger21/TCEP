import os
import pandas as pd
from collections import OrderedDict
import numpy as np

NUM_SECTIONS = 2


# loads the madrid tracefile and uses pandas to modify it for use in mobility simulation of publishers (RegularPublisher.scala)

# load tracefile consisting of rows of <time, id, position, lane, speed> into dataframe
def loadTrace(file):
    data = pd.read_csv(file, sep=' ', header=None)
    data.columns = ["time", "id", "position", "lane", "speed"]
    df = pd.DataFrame(data)
    print("loaded tracefile: " + file)
    return df

def loadLinearRoadTrace(file):
    data = pd.read_csv(file, sep=",")
    return data

def getRoadEnd(df):
    return df["position"].max()


def getRoadBeginning(df):
    return df["position"].min()


def getRoadLength(df):
    return getRoadEnd(df) - getRoadBeginning(df)


# calculate the length of one road section
def getSectionLength(df, sections):
    return getRoadLength(df) / sections


# find starting point: first time when a car reaches the end of the road, so that simulation starts with vehicles on all road sections
def getStartingTime(df):
    maxpos = getRoadEnd(df)
    sorted = df[df.position >= maxpos - 20].sort_values("time", ascending=True)
    return sorted.iat[0, 0]


def getMaxPosition(df, id):
    groupedMaxPositions = df["position"].groupby([df["id"]]).max()
    return groupedMaxPositions[id]


# return time when id leaves the road; return -1 if it is still on the road when the trace ends
def getExitTime(df, id):
    if getMaxPosition(df=df, id=id) >= getRoadEnd(df) - 20:
        return df[df.id == id].sort_values("position", ascending=False).iat[0, 0]
    else:
        return -1


def writeTraceFiles(tracedict, dir):
    i = 1
    for key in tracedict:
        tracedict[key].to_csv("%s/speedTrace%s.csv" % (dir, i),
                              sep=",", columns=["time", "position", "lane", "speed"], index=False)
        i += 1

def write_linear_road_traces(publisher_traces,dir):
    for key in publisher_traces:
        i = key+1
        publisher_traces[key].to_csv("%s/linearRoadP%s.csv" % (dir, i), sep=",", index=False)

def load_linear_road_traces(dir, time_between, num_pub):
    d = {}
    for i in range(1, num_pub+1):
        df = pd.read_csv("%s/linearRoadT%s_P%s.csv" % (dir, time_between, i), sep=",")
        d[i] = df
    return d

def densityCalculationNecessary(dir):
    if os.path.exists("%s/densityTrace1_%s.csv" % (dir, NUM_SECTIONS)):
        return False
    else:
        return True

def writeDensityTraceFiles(densityDict, dir):
    i = 1
    for key in densityDict:
        densityDict[key].to_csv("%s/densityTrace%s_%s.csv" % (dir, i, NUM_SECTIONS),
                                sep=",", index=False)
        i += 1

# when a vehicle (publisher) leaves the road before the simulation is over, it needs a replacement vehicle id
def findReplacementId(df, exitTime):
    # find an id that is entering the first section during exitTime
    filtered = df[(df.time == exitTime) & (df.position < getRoadBeginning(df) + 100)]
    repId = filtered.iloc[0, 1]
    repTrace = df[(df.id == repId) & (df.time > exitTime)]
    return repId, repTrace


# check if the vehicle is still on the road when the simulation ends
def checkTraceExtension(df, id, ancestorid, traces):
    exitTime = getExitTime(df=df, id=id)
    if exitTime >= 0:
        rid, rtrace = findReplacementId(df, exitTime)
        traces[ancestorid] = traces[ancestorid].append(rtrace)
        # print exitTime
        # print traces[ancestorid][(df.time > exitTime - 1) & (df.time < exitTime + 1)]
        checkTraceExtension(df=df, id=rid, ancestorid=ancestorid, traces=traces)

    return traces


# insert a full stop (stop all vehicles' movement) for 5 minutes after half the simulation duration
# -> additional significant context change
def insertFullStop(tracedict):
    for key in tracedict:

        trace = tracedict[key]
        start = trace["time"].max() / 2
        stopDuration = 300.0

        trace_1 = trace.loc[trace.time <= start]
        trace_2 = trace.loc[trace.time > start]
        id = trace_1["id"].loc[start]
        pos = trace_1["position"].loc[start]
        lane = trace_1["lane"].loc[start]
        speed = 0.0
        trace_1.reset_index(drop=True, inplace=True)
        trace_2.reset_index(drop=True, inplace=True)

        for i in range(1, int(stopDuration * 2), 1):
            timefloat = start + float(i) / 2
            line = [(timefloat, id, pos, lane, speed)]
            label = ["time", "id", "position", "lane", "speed"]
            frame = pd.DataFrame.from_records(line, columns=label)
            trace_1 = trace_1.append(frame, ignore_index=True)

        # print trace_1
        trace_2["time"] = trace_2["time"].apply(lambda x: x + stopDuration)
        result = trace_1.append(trace_2, ignore_index=True)
        # print result["position"].loc[1456:2070]

        result.set_index("time", inplace=True, drop=False)
        tracedict[key] = result

    return tracedict


#  insert a full stop (stop all vehicles' movement) for 5 minutes at simulation start
def insertFullStopAtStart(tracedict):
    for key in tracedict:

        trace = tracedict[key]
        start = trace["time"].min()
        stopDuration = 60.0
        # print start, stopDuration

        trace_1 = trace.loc[trace.time <= start]
        trace_2 = trace.loc[trace.time > start]
        id = trace_1["id"].loc[start]
        pos = trace_1["position"].loc[start]
        lane = trace_1["lane"].loc[start]
        speed = 0.0
        trace_1.reset_index(drop=True, inplace=True)
        trace_2.reset_index(drop=True, inplace=True)

        for i in range(1, int(stopDuration * 2) + 1, 1):
            # we need to make steps of 0.5s
            timefloat = start + float(i) / 2
            line = [(timefloat, id, pos, lane, speed)]
            label = ["time", "id", "position", "lane", "speed"]
            frame = pd.DataFrame.from_records(line, columns=label)
            trace_1 = trace_1.append(frame, ignore_index=True)

        trace_2["time"] = trace_2["time"].apply(lambda x: x + stopDuration)
        result = trace_1.append(trace_2, ignore_index=True)

        result.set_index("time", inplace=True, drop=False)
        tracedict[key] = result

    return tracedict

def generateDensityTracefiles(file, sections, stopAtStart=True):
    dir = "/".join(file.split("/")[:-1])
    if densityCalculationNecessary(dir):
        df = loadTrace(file)
        section_length = getSectionLength(df=df, sections=sections)
        density_dict = {}
        for sec in range(sections):
            times = np.arange(0.0, np.max(df.time), 0.5)
            densities = []
            start_pos = sec*section_length
            end_pos = (sec+1)*section_length
            for t in times:
                density = df[(df.time==t)&(df.position>start_pos)&(df.position <= end_pos)]["id"].unique().shape[0]
                densities.append(density)
            if stopAtStart:
                stopDuration=60.0
                density = densities[0]
                tmpDensities = []
                for t in np.arange(0.0, stopDuration, 0.5):
                    tmpDensities.append(density)
                densities = tmpDensities+densities
                times = np.arange(0.0, np.max(df.time)+stopDuration, 0.5)
            denDf = pd.DataFrame()
            denDf["time"] = times
            denDf["density"] = densities
            density_dict[sec] = denDf
        writeDensityTraceFiles(density_dict,dir)

# load a tracefile and modify it for use as publisher mobility simulation input
def generateTraceDataFromFile(file, sections, vehiclesPerSection, stopAtHalf=False, stopAtStart=False):
    dir = "/".join(file.split("/")[:-1])
    df = loadTrace(file)
    df = df[df.time > getStartingTime(df)]
    tracesdict = OrderedDict()
    sectionLength = getSectionLength(df=df, sections=sections)
    for i in range(0, sections):
        sectionStart = i * sectionLength + getRoadBeginning(df)
        sectionEnd = i * sectionLength + sectionLength + getRoadBeginning(df)
        # entries from vehicles that are between 0 and 75% of the section length and on the middle lane at the start time
        sectionAtStartTime = df[(df.position >= sectionStart) &
                                (df.position < sectionEnd - (0.1 * sectionLength)) &
                                (df.time == getStartingTime(df))]
        # pick n random ids
        # samples = sectionAtStartTime.sample(vehiclesPerSection)

        entries = len(sectionAtStartTime.index)
        step = entries / vehiclesPerSection
        # pick n ids from the section at start time
        for s in range(0, vehiclesPerSection):
            # id = samples["id"].iloc[s]
            ind = int(s * step)
            id = sectionAtStartTime["id"].iloc[ind]
            trace = df[df["id"] == id]
            tracesdict[id] = trace

    for key in tracesdict:
        # handle cases where ids reach the end of the road during the simulation
        # -> find a replacement id at the first road section at that time,
        # repeat until there is an id that is still on the road when the trace ends
        tracesdict = checkTraceExtension(df=df, id=key, ancestorid=key, traces=tracesdict)

    startingTime = getStartingTime(df)
    minpos = getRoadBeginning(df)
    # correct time and position values so they start at 0
    for key in tracesdict:
        tracesdict[key]["time"] = tracesdict[key]["time"].apply(lambda x: x - startingTime)
        tracesdict[key]["position"] = tracesdict[key]["position"].apply(lambda x: x - minpos)
        tracesdict[key].set_index("time", inplace=True, drop=False)

    if stopAtHalf == True:
        tracesdict = insertFullStop(tracesdict)

    if stopAtStart == True:
        tracesdict = insertFullStopAtStart(tracesdict)

    i = 1
    res = OrderedDict()
    for key in tracesdict:
        res[i] = tracesdict[key]
        i += 1

    print("writing individual trace for publishers to %s" % dir)
    writeTraceFiles(tracesdict, dir)
    return res, sectionLength


def load_traces(num_cars, cars_per_rsu, trace_file, stop_at_start=True, stop_at_half=False, ):
    print('*** loading and pre-processing mobility trace file %s \n' % trace_file)
    # rsu: Road-Side-Unit
    assert cars_per_rsu >= 1 & cars_per_rsu <= num_cars, 'there must be at least one car per rsu'
    assert (float(num_cars) / cars_per_rsu) % 1 == 0, 'number of cars must be distributable to rsus without remainder'
    data = loadTrace(trace_file)
    road_length = getRoadLength(data)
    num_rsus = int(num_cars / cars_per_rsu)
    rsu_range = (road_length / num_rsus) / 2
    # dictionary with keys 1 to num_rsus * cars_per_rsu
    # values trace data frame (time, position, speed) for each vehicle
    traces, section_length = generateTraceDataFromFile(file=trace_file,
                                                       sections=num_rsus, vehiclesPerSection=cars_per_rsu,
                                                       stopAtHalf=stop_at_half, stopAtStart=stop_at_start)
    traces_2, _ = generateTraceDataFromFile(file=trace_file,
                                                       sections=num_rsus, vehiclesPerSection=cars_per_rsu,
                                                       stopAtHalf=False, stopAtStart=False)
    for key in traces:
        first_end = traces[key].time.max()+0.5
        times = traces_2[key].time
        tmp = traces_2[key]
        tmp["time"] = times+first_end
        tmp.index = tmp.time
        traces[key] = pd.concat((traces[key],tmp), axis=0)
    generateDensityTracefiles(file=trace_file, sections=NUM_SECTIONS, stopAtStart=stop_at_start)
    print("road length: %s\n number of rsus: %s\n rsu range: %s\n section length: %s" % (
        road_length, num_rsus, rsu_range, section_length))
    assert section_length == 2 * rsu_range, 'section length (%s) should equal double rsu range (%s)' % (
        section_length, 2 * rsu_range)
    return traces, rsu_range, num_rsus, road_length


# load_traces(8, 2, tace_file='A6-d11-h08.dat', stop_at_start=True, stop_at_half=False)
