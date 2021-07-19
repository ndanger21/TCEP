#!/usr/bin/env python
# coding: utf-8

# In[3]:
import collections
import os

os.environ['OPENBLAS_NUM_THREADS'] = '1'
import time
import pandas as pd
import matplotlib.pyplot as plt
from river import linear_model, tree, ensemble, stream, metrics, preprocessing, compose
import seaborn as sns

if __name__ == "__main__":
    filename = '/home/niels/Downloads/data_collection/mininet_accident/sampling_1s/mininet_accident_1s_combined_samples.csv'
    logfile = 'yahoo_geni_1s_logs.csv'
    #filename = "stats-Conjunction11bf7aeaea-87321827.csv"
    f = open(filename)
    df = pd.read_csv(f, sep=";", header=0)
    f.close()
    with open(logfile) as l:
        log_df = pd.read_csv(l, sep="\t", header=0)
        print(log_df.columns)
        log_df.columns = log_df.columns.str.strip()  # fix messy whitespace in column names
        plt.plot(log_df["latency"], label="total e2e latency")
        plt.grid()
        plt.xlabel('time [s]')
        plt.ylabel('query e2e latency [ms]')
        plt.show()

        plt.clf()
        plt.plot(log_df["eventArrivalRate"], label="event arrivals at consumer")
        plt.grid()
        plt.xlabel('time [s]')
        plt.ylabel('arrived events / second')
        plt.show()

    df = df._get_numeric_data()
    #print(df)
    numeric_headers = list(df.columns.values)
    #print(numeric_headers)
    #df = df[ np.abs(df['e2eLatencyMean'] - df['e2eLatencyMean'].mean(axis=0)) <= 2 * df['e2eLatencyMean'].std(axis=0) ]
    #df['processingLatencyMean'] = df['processingLatencyMean'] * 1000
    plt.clf()
    plt.plot(df["processingLatencyMean"], label="processingLatencyMean", color="limegreen")
    plt.grid()
    plt.legend()
    plt.ylabel("processingLatencyMean")
    plt.xlabel("sample")
    plt.show()

    plt.clf()
    plt.plot(df["eventRateOut"], label="eventRateOut", color="red")
    plt.grid()
    plt.legend()
    plt.ylabel("eventRateOut")
    plt.xlabel("sample")
    plt.show()

    df.to_csv('/tmp/samples_with_comma.csv', sep=',', index=None)


# In[4]:


online_models_base = [
    linear_model.LinearRegression(),
    linear_model.PARegressor(),
    tree.HoeffdingTreeRegressor(),
    tree.HoeffdingAdaptiveTreeRegressor(),  # +ADWIN concept-drift detector
    ensemble.AdaptiveRandomForestRegressor()
    #neural_net.MLPRegressor(
    #  hidden_dims=(10,),
    #  activations=(
    #      activations.ReLU,
    #      activations.ReLU,
    #      activations.Identity
    #  ))
    ]

target_features = ['eventRateOut', 'processingLatencyMean']
online_models = {name: online_models_base.copy() for name in target_features }

selected_features_throughput = compose.Select(
    'Relaxation', 'Rizou', 'ProducerConsumer', 'Random', 'GlobalOptimalBDP', 'MDCEP',
    'operatorSelectivity', 'eventRateIn', 'processingLatencyMean', 'processingLatencyStdDev')
selected_features_processing_latency = compose.Select(
    'Relaxation', 'Rizou', 'ProducerConsumer', 'Random', 'GlobalOptimalBDP', 'MDCEP',
    'eventSizeInKB', 'operatorSelectivity', 'interArrivalMean', 'interArrivalStdDev', 'brokerCPULoad', 'eventRateIn')
selected_dict = {
    'eventRateOut': selected_features_throughput,
    'processingLatencyMean': selected_features_processing_latency
}
params = {
'converters': {
    'Relaxation' : int,
    'Rizou' : int,
    'ProducerConsumer': int,
    'Random' : int,
    'GlobalOptimalBDP': int,
    'MDCEP': int,
    'eventSizeInKB': float,
    'operatorSelectivity': float,
    'interArrivalMean': float,
    'interArrivalStdDev': float,
    'brokerCPULoad': float,
    'eventRateIn': float,
    'eventRateOut': float,
    'processingLatencyMean': float,
    'processingLatencyStdDev': float,
    }
}


#for x, y in stream.iter_csv('/tmp/samples_with_comma.csv', target=target_features[0], **params):
#    print(x, y)
plt.plot(clear=True)

#TODO expert.SuccessiveHalvingRegressor()


def rmse_last60(q):
    squares = map(lambda t: (t[0] - t[1]) ** 2, q)
    return (sum(list(squares)) / q.__len__()) ** 0.5

def mae_last60(q):
    return sum(list(map(lambda t: abs(t[0] - t[1]), q))) / q.__len__()

for t in target_features:
    print("new target: ", t)
    loss_over_last60 = {}
    for model in online_models[t]:
        if t == 'processingLatencyMean' and isinstance(model, ensemble.AdaptiveRandomForestRegressor):
            print("skipping AdaptiveRandomForestRegressor for ", t)
            continue
        loss_over_last60[model] = {'mae': [] }
        loss_over_last60[model]['rmse'] = []
        # metric = metrics.MAE()
        metric = metrics.RMSE()
        
        print("\ncurrent model: ", model, "current target: ", t)
        pipeline = selected_dict[t]
        pipeline |= preprocessing.StandardScaler()
        pipeline |= model
        #print("model init finished")
        #print(pipeline)

        #print("starting progressive eval")

        #perf = evaluate.progressive_val_score(
        #    stream.iter_csv('/tmp/samples_with_comma.csv', target=t, **params),
        #    pipeline, metric, print_every=10)
        #print("model perf is ", perf)
        i = 0
        #last60 = queue.Queue(60)
        last60 = collections.deque(maxlen=60)

        for x, y in stream.iter_csv('/tmp/samples_with_comma.csv', target=t, **params):
            start = time.time()
            y_pred = pipeline.predict_one(x)
            predict_time = time.time()
            #if i >= 60 and y_pred != {} and y_pred is not None:
            metric.update(y_true=y, y_pred=y_pred)

            if last60.__len__() == 60:
                loss_over_last60[model]['rmse'].append(rmse_last60(last60))
                loss_over_last60[model]['mae'].append(mae_last60(last60))
                last60.pop()

            last60.appendleft((y, y_pred))

            pipeline.learn_one(x=x, y=y)
            #print("sample ", i, " | truth: ", y, " prediction: ", y_pred, " metric: ", metric.get()) #if i % 1000 == 0 else None
            i += 1
            end = time.time()
            if i % 10000 == 0:
                print("predict took %s, learn took %s" % (predict_time - start, end - predict_time))
                print(x, y)

        print("total loss on all samples: %s - %s" % (model, metric.get()))
        print("loss on last 60 samples: rmse %s | mae %s" % (loss_over_last60[model]['rmse'][-1], loss_over_last60[model]['mae'][-1]))

    plt.clf()
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 24))
    for model, losses in loss_over_last60.items():
        plt.plot(losses['rmse'], label="%s" % model, ax=axes)
        plt.plot(losses['mae'], label="%s" % model, ax=axes)
    plt.grid()
    plt.legend()
    plt.ylabel("RMSE on %s" % t)
    plt.xlabel("sample")
    plt.show()

    with sns.plotting_context({"font.size": 16.0}):
        fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(4*3,3*4))
        sns.boxplot(x="model", y="loss", hue="Algorithm", data=loss_over_last60[online_models[0]['rmse']])
        sns.boxplot(x="model", y="loss", hue="Algorithm", data=loss_over_last60[online_models[1]['rmse']], ax=axes[1])
        sns.boxplot(x="model", y="loss", hue="Algorithm", data=loss_over_last60[online_models[2]['rmse']], ax=axes[2])
        for ax in axes:
            ax.grid()
            ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        #axes[0].set_title(beautifyDatasetString["madrid"])
        #axes[1].set_title(beautifyDatasetString["linear"])
        #axes[2].set_title(beautifyDatasetString["yahoo"])
        #axes.legend(loc="center left", bbox_to_anchor=(1, 0.5))
        #axes.grid()
        fig.tight_layout()
# In[ ]:




