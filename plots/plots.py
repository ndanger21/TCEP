import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'

import pandas as pd
#import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":

    filename = "../prediction/stats-Average23cb031d5--1593549630.csv"
    f = open(filename)
    df = pd.read_csv(f, sep=";", header=0)
    f.close()
    df = df._get_numeric_data()
    print(df)
    numeric_headers = list(df.columns.values)
    numeric_headers.remove("brokerOtherBandwidthInKB")
    numeric_headers.remove("brokerOtherBandwidthOutKB")
    #print(numeric_headers)

    #plt.plot(df["e2eLatencyMean"], label="e2eLatencyMean", color="limegreen")
    for col in numeric_headers:
        plt.plot(df[col], label=col)

    plt.semilogy()
    plt.grid()
    plt.legend()
    plt.ylabel("Latency")
    plt.xlabel("Time")
    plt.show()

    """
    ci="sd"
    with sns.plotting_context({"font.size": 16.0}):
        fig, axes = plt.subplots(nrows=2, ncols=3, figsize=(3*6.5+3,2*5+2))
        sns.lineplot(x="Time", y="mLatency", data=madrid, estimator=np.mean, ci=ci, ax=axes[0][0])
        sns.lineplot(x="Time", y="mLatency", data=linear, estimator=np.mean, ci=ci, ax=axes[0][1])
        sns.lineplot(x="Time", y="mLatency", data=yahoo, estimator=np.mean, ci=ci, ax=axes[0][2])
        sns.lineplot(x="Time", y="mAvgSystemLoad", data=madrid, estimator=np.mean, ci=ci, ax=axes[1][0])
        sns.lineplot(x="Time", y="mAvgSystemLoad", data=linear, estimator=np.mean, ci=ci, ax=axes[1][1])
        sns.lineplot(x="Time", y="mAvgSystemLoad", data=yahoo, estimator=np.mean, ci=ci, ax=axes[1][2])
        axes[0][0].set_title("Madrid")
        axes[0][1].set_title("Linear Road")
        axes[0][2].set_title("Yahoo")
        axes[0][0].set_ylim(-100, 3000)
        axes[0][1].set_ylim(-100, 3000)
        axes[0][2].set_ylim(-100, 3000)
        axes[1][0].set_ylim(-1, 20)
        axes[1][1].set_ylim(-1, 20)
        axes[1][2].set_ylim(-1, 50)
        for ax in axes:
            for a in ax:
                a.grid()
        fig.tight_layout()
        """