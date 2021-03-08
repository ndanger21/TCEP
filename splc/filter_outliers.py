import numpy as np
import pandas as pd
import sys

file = sys.argv[1]
# applied to individual simulation runs
# filter latency spikes caused by machine overload
def filterOutliers():
    print "filtering file: " +file
    data = pd.read_csv(file, sep=';', header=0)
    df = pd.DataFrame(data)
    filtered = df[np.abs(df['mLatency'] - df['mLatency'].mean(axis=0)) <= 3 * df['mLatency'].std(axis=0)]
    filtered = filtered * 1 # multiply all cells by 1 -> convert True/False to 1/0 (attributes remain equal) to avoid SPLC parsing problems
    filtered.to_csv(file, index=False, sep=';')

filterOutliers()