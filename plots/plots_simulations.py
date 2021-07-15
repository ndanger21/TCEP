import fnmatch
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import seaborn as sns
import sys
from collections import OrderedDict
from matplotlib import style
from matplotlib.ticker import (MultipleLocator, FuncFormatter, ScalarFormatter)


###########################################################################################################
# data loading
###########################################################################################################

def loadFile(file_to_load):
    #print file_to_load
    data = pd.read_table(file_to_load, sep=',', header=None)
    data.columns = ["time","latency","hops","load","publishing_rate","arrival_rate", "transition_status", "msg_overhead",
                    "network_usage",
                    # p = prediction
                    "pietzuch_latency_p", "pietzuch_load_p", "pietzuch_hops_p", "pietzuch_overhead_p",
                    "starks_latency_p", "starks_load_p", "starks_hops_p", "starks_overhead_p",
                    "random_latency_p", "random_load_p", "random_hops_p", "random_overhead_p",
                    "mob_latency_p", "mob_load_p", "mob_hops_p", "mob_overhead_p",
                    "link_changes", "active_algorithm"]
    frame = pd.DataFrame(data)
    frame.set_index("time", inplace=True)
    #frame = frame.loc[:720]

    return frame


def combine_data(data_list, name):

    t_latencies      = pd.DataFrame()
    t_hops           = pd.DataFrame()
    t_load           = pd.DataFrame()
    t_overhead       = pd.DataFrame()
    t_pred_latencies = pd.DataFrame()
    t_pred_hops      = pd.DataFrame()
    t_pred_load      = pd.DataFrame()
    t_pred_overhead  = pd.DataFrame()
    t_link_changes   = pd.DataFrame()

    for i in range(0, len(data_list)):
        t_latencies["l"+str(i)]       = data_list[i]["latency"]
        t_hops["h"+str(i)]            = data_list[i]["hops"]
        t_load["load"+str(i)]         = data_list[i]["load"]
        t_overhead["o"+str(i)]        = data_list[i]["msg_overhead"]
        t_pred_latencies["l"+str(i)]  = data_list[i][str(name)+"_latency_p"]
        t_pred_hops["h"+str(i)]       = data_list[i][str(name)+"_hops_p"]
        t_pred_load["load"+str(i)]    = data_list[i][str(name)+"_load_p"]
        t_pred_overhead["o"+str(i)]   = data_list[i][str(name)+"_overhead_p"]
        t_link_changes["lc"+str(i)]   = data_list[i]["link_changes"]
    res = { "latencies_p" : t_pred_latencies, "hops_p" : t_pred_hops, "load_p" : t_pred_load, "overhead_p" : t_pred_overhead,
            "latencies" : t_latencies, "hops" : t_hops, "load" : t_load, "overhead" : t_overhead,
            "link_changes" : t_link_changes}
    return res


def load_data(sim_folder_name):
    path = str(sim_folder_name)
    pietzuch_files = []
    starks_files = []
    random_files = []
    mob_files = []

    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, '*Pietzuch.csv'):
            pietzuch_files.append(os.path.join(root, filename))

    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, '*Starks.csv'):
            starks_files.append(os.path.join(root, filename))

    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, '*Random.csv'):
            random_files.append(os.path.join(root, filename))

    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, '*MobilityTolerant.csv'):
            mob_files.append(os.path.join(root, filename))

    pietzuch_data = []
    starks_data = []
    random_data = []
    mob_data = []

    for file in pietzuch_files:
        df = loadFile(file)
        #filter latency outliers, i.e. only keep values within 3 std deviations
        for i in ["latency", "pietzuch_latency_p"]:
            df[i] = df[i][np.abs(df[i] - df[i].mean(axis=0)) <= (3 * df[i].std(axis=0))]
        pietzuch_data.append(df)

    for file in starks_files:
        df = loadFile(file)
        for i in ["latency", "starks_latency_p"]:
            df[i] = df[i][np.abs(df[i] - df[i].mean(axis=0)) <= (3 * df[i].std(axis=0))]
        starks_data.append(df)

    for file in random_files:
        df = loadFile(file)
        for i in ["latency", "random_latency_p"]:
            df[i] = df[i][np.abs(df[i] - df[i].mean(axis=0)) <= (3 * df[i].std(axis=0))]
        random_data.append(df)

    for file in mob_files:
        df = loadFile(file)
        for i in ["latency", "mob_latency_p"]:
            df[i] = df[i][np.abs(df[i] - df[i].mean(axis=0)) <= (3 * df[i].std(axis=0))]
        mob_data.append(df)

    pietzuch_comb = combine_data(pietzuch_data, "pietzuch")
    starks_comb = combine_data(starks_data, "starks")
    random_comb = combine_data(random_data, "random")
    mob_comb = combine_data(mob_data, "mob")

    #print pietzuch_comb["latencies"].mean(axis=1).describe()
    #print starks_comb["latencies"].mean(axis=1).describe()
    #print random_comb["latencies"].mean(axis=1).describe()

    #print pietzuch_comb["load"].mean(axis=1).describe()
    #print starks_comb["load"].mean(axis=1).describe()
    #print random_comb["load"].mean(axis=1).describe()

    #print pietzuch_comb["hops"].mean(axis=1).describe()
    #print starks_comb["hops"].mean(axis=1).describe()
    #print random_comb["hops"].mean(axis=1).describe()

    return pietzuch_comb, starks_comb, random_comb, mob_comb

###########################################################################################################

def relative_error(values, approximations):
    abs_error = values.subtract(approximations).abs()
    return abs_error.divide(values.abs())

def line_plot_metric_two_axes(data, pred_data, stddev=False):
    if data.empty: return
    if pred_data.empty: return
    plt.clf()
    style.use('seaborn')

    #ax = sns.tsplot(data["avg"], data["time"], color='r')
    #ax = sns.tsplot(pred_data["avg"], pred_data["time"], color="g")
    #data["const"] = 400
    #ax = sns.tsplot(data["const"], data["time"], color='b')
    #print data
    ax = data.mean(axis=1).plot()
    x = data.index
    low = data.mean(axis=1) - data.std(axis=1)
    high = data.mean(axis=1) + data.std(axis=1)
    palette = sns.color_palette()
    if stddev: ax.fill_between(x, low, high, alpha=0.2, color=palette.pop(0))

    ax = pred_data.mean(axis=1).plot()
    x = pred_data.index
    pred_low = pred_data.mean(axis=1) - pred_data.std(axis=1)
    pred_high = pred_data.mean(axis=1) + pred_data.std(axis=1)
    palette = sns.color_palette()
    ax.fill_between(x, pred_low, pred_high, alpha=0.2, color=palette.pop(1))
    #ax.errorbar(x=pred_data.index, y=pred_data.mean(axis=1), yerr=pred_data.std(axis=1), fmt='-')
    return plt


def bar_plot_metric(data, algorithm, metric, foldername):
    if data.empty: return

    plt.clf()
    style.use('seaborn')

    #pandas
    ax = data.plot(kind='bar')
    x1,x2,y1,y2 = plt.axis()
    ymin = data.min().min()
    plt.axis((x1,x2, ymin * 0.9, y2))
    # print label every 6 entries
    ax.xaxis.set_major_locator(MultipleLocator(6))
    sf = ScalarFormatter()
    sf.create_dummy_axis()
    sf.set_locs((data.index.max(), data.index.min()))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x,p: sf((x * 5) + data.index[0])))
    ax.legend(['actual value', 'estimated value'],loc='upper center')
    plt.xlabel('Time [s]')
    plt.ylabel(metric)

    plt.savefig(str(foldername)+"/"+metric+"_"+algorithm+"_bar")
    plt.clf()
    plt.close()


#############################################################################################################

def generate_metric_plots_with_estimates(data_dict, name, foldername):
    if data_dict["latencies"].empty: return
    plt = line_plot_metric_two_axes(data_dict["latencies"], data_dict["latencies_p"])
    x1,x2,y1,y2 = plt.axis()
    plt.axis((x1,x2, y1, 500))
    plt.xlabel('Time [s]')
    plt.ylabel('Average Latency [ms]')
    plt.legend(['avg actual latency', 'avg estimated latency'], loc='upper left')
    #plt.axvspan(300, 600, alpha=0.5, label = "Transition")
    plt.savefig(str(foldername)+"/"+"latency_avg_"+str(name))
    #plt.show()
    plt.clf()

    plt = line_plot_metric_two_axes(data_dict["load"], data_dict["load_p"])
    x = [300, 600]
    y = [1.0, 1.0]
    #plt.plot(x, y)
    plt.xlabel('Time [s]')
    plt.ylabel('Average Load [cpu share]')
    plt.legend(['avg actual load', 'avg estimated load', 'load requirement'], loc='upper right')
    #plt.axvspan(300, 600, alpha=0.5, label = "Transition")
    plt.savefig(str(foldername)+"/"+"load_avg_"+str(name))
    plt.clf()

    plt = line_plot_metric_two_axes(data_dict["hops"], data_dict["hops_p"])
    #x = [600, 830]
    #y = [5.0, 5.0]
    #plt.plot(x, y)
    plt.xlabel('Time [s]')
    plt.ylabel('Average Node Hops [#]')
    #plt.legend(['avg actual hops', 'avg estimated hops', 'hops requirement'], loc='upper right')
    #plt.axvspan(300, 600, alpha=0.5, label = "Transition")
    plt.savefig(str(foldername)+"/"+"hops_avg_"+str(name))
    plt.clf()

def generate_rel_error_plots(pietzuch, starks, mob, foldername):
    # generate plot with 3 curves: relative prediction error of latency, load, hops
    keys = [("latencies", "latencies_p"), ("hops", "hops_p"), ("load", "load_p")]

    rel_error_pietzuch = {
        "latencies" : pd.DataFrame(index=pietzuch["latencies"].index),
        "hops" : pd.DataFrame(index=pietzuch["latencies"].index),
        "load" : pd.DataFrame(index=pietzuch["latencies"].index) }
    rel_error_starks = {
        "latencies" : pd.DataFrame(index=pietzuch["latencies"].index),
        "hops" : pd.DataFrame(index=pietzuch["latencies"].index),
        "load" : pd.DataFrame(index=pietzuch["latencies"].index) }
    rel_error_mob = {
        "latencies" : pd.DataFrame(index=pietzuch["latencies"].index),
        "hops" : pd.DataFrame(index=pietzuch["latencies"].index),
        "load" : pd.DataFrame(index=pietzuch["latencies"].index) }

    for pair in keys:

        for i in pietzuch[pair[0]].columns:
            rel_error_pietzuch[pair[0]][i] = relative_error(pietzuch[pair[0]][i], pietzuch[pair[1]][i])

        for i in starks[pair[0]].columns:
            rel_error_starks[pair[0]][i] = relative_error(starks[pair[0]][i], starks[pair[1]][i])

        for i in mob[pair[0]].columns:
            rel_error_mob[pair[0]][i] = relative_error(mob[pair[0]][i], mob[pair[1]][i])

    # mean relative error per run
    latency_bars = OrderedDict()
    latency_error_std = OrderedDict()
    hops_bars = OrderedDict()
    load_bars = OrderedDict()
    for col in rel_error_pietzuch["latencies"]:
        latency_bars["p"+col] = rel_error_pietzuch["latencies"][col].mean()
        latency_error_std["p"+col] = rel_error_pietzuch["latencies"][col].std()

    for col in rel_error_starks["latencies"]:
        latency_bars["s"+col] = rel_error_starks["latencies"][col].mean()
        latency_error_std["s"+col] = rel_error_starks["latencies"][col].std()

    for col in rel_error_mob["latencies"]:
        latency_bars["m"+col] = rel_error_mob["latencies"][col].mean()
        latency_error_std["m"+col] = rel_error_mob["latencies"][col].std()


    plt.clf()
    #bar plot
    err = pd.Series(latency_error_std.values())

    print err
    ax = plt.bar(range(len(latency_bars)), list(latency_bars.values()), yerr=list(err))
    ax = plt.xticks(range(len(latency_bars)), list(latency_bars.keys()))
    plt.xlabel('Time [s]')
    plt.ylabel('Avg Relative Error per Sim')
    plt.savefig(str(foldername)+"/"+"avg_rel_prediction_error_bar")


    plt.clf()
    combined_latency_errors = pd.concat([rel_error_pietzuch["latencies"], rel_error_starks["latencies"], rel_error_mob["latencies"]], axis=1)
    combined_hops_errors = pd.concat([rel_error_pietzuch["hops"], rel_error_starks["hops"], rel_error_mob["hops"]], axis=1)
    combined_load_errors = pd.concat([rel_error_pietzuch["load"], rel_error_starks["load"], rel_error_mob["load"]], axis=1)

    ax = combined_latency_errors.mean(axis=1).plot()
    x = combined_latency_errors.index
    #low = combined_latency_errors.mean(axis=1) - combined_latency_errors.std(axis=1)
    #high = combined_latency_errors.mean(axis=1) - combined_latency_errors.std(axis=1)
    #palette = sns.color_palette()
    #ax.fill_between(x, low, high, alpha=0.2, color=palette.pop(0))

    ax = combined_hops_errors.mean(axis=1).plot()
    x = combined_hops_errors.index
    #low = combined_hops_errors.mean(axis=1) - combined_hops_errors.std(axis=1)
    #high = combined_hops_errors.mean(axis=1) - combined_hops_errors.std(axis=1)
    #palette = sns.color_palette()
    #ax.fill_between(x, low, high, alpha=0.2, color=palette.pop(1))

    ax = combined_load_errors.mean(axis=1).plot()
    x = combined_load_errors.index
    #low = combined_load_errors.mean(axis=1) - combined_load_errors.std(axis=1)
    #high = combined_load_errors.mean(axis=1) - combined_load_errors.std(axis=1)
    #palette = sns.color_palette()
    #ax.fill_between(x, low, high, alpha=0.2, color=palette.pop(2))

    plt.xlabel('Time [s]')
    plt.ylabel('Avg Relative Error')
    plt.legend(['Latency', 'Hops', 'Load'], loc='upper center')
    #plt.axvspan(300, 600, alpha=0.5, label = "Transition")
    plt.savefig(str(foldername)+"/"+"avg_rel_prediction_error")
    plt.clf()

    #for i in pietzuch["latencies"].keys():
    #    pietzuch["latencies"][i].plot()
    #    pietzuch["latencies_p"][i].plot()
    #    plt.show()
    #pietzuch["latencies"].mean(axis=1).plot()
    #pietzuch["latencies_p"].mean(axis=1).plot()
    #print pietzuch["latencies"].describe()
    #plt.show()
    #starks["latencies"].mean(axis=1).plot()
    #starks["latencies_p"].mean(axis=1).plot()
    #print mob["latencies"].describe()
    #plt.show()
    #mob["latencies"].mean(axis=1).plot()
    #mob["latencies_p"].mean(axis=1).plot()
    #print mob["latencies"].describe()
    #plt.show()
    #ax = relative_error(mob["latencies"]["l0"], mob["latencies_p"]["l0"]).plot()
    #ax = relative_error(starks["latencies"]["l0"], starks["latencies_p"]["l0"]).plot()
    #ax = relative_error(pietzuch["latencies"]["l0"], pietzuch["latencies_p"]["l0"]).plot()
    #plt.show()


# actual value, predicted value, requirement value
def calculate_metrics(actual, pred):

    accuracy = []
    precision = []
    recall = []

    for run in actual:

        descr = actual[run].describe()
        print descr
        min = descr.loc["25%"]
        max = descr.loc["75%"]
        diff = max - min
        step = int(diff / 6)
        req_vals = range(min, max, step)
        print diff, step, req_vals
        for reqval in req_vals:
            tp = actual[run][(actual[run] <= reqval) & (pred[run] <= reqval)].count()
            fp = actual[run][(actual[run] > reqval) & (pred <= reqval)].count()
            fn = actual[run][(actual[run] <= reqval) & (pred > reqval)].count()
            tn = actual[run][(actual[run] > reqval) & (pred > reqval)].count()
            print tp, fp, fn, tn
            acc[reqval] = float(tp + tn) / float(tp + tn + fp + fn)
            prec[reqval] = float(tp) / float(tp + fp)
            rec[reqval] = float(tp) / float(tp + fn)
            print acc, prec, rec
            accuracy.append(acc)
            precision.append(prec)
            recall.append(rec)

    # avg per req group
    return accuracy_avg, precision_avg, recall_avg


def generate_prediction_accuracy_errorbar_plots(pietzuch, starks, random, mob):
    keys = [("latencies", "latencies_p"), ("hops", "hops_p"), ("load", "load_p")]

    print pietzuch["latencies"]
    combined = OrderedDict()
    combined["latencies"] = pd.concat([pietzuch["latencies"], starks["latencies"], random["latencies"], mob["latencies"]], axis=1)
    combined["hops"] = pd.concat([pietzuch["hops"], starks["hops"], random["hops"], mob["hops"]], axis=1)
    combined["load"] = pd.concat([pietzuch["load"], starks["load"], random["load"], mob["load"]], axis=1)
    combined["latencies_p"] = pd.concat([pietzuch["latencies_p"], starks["latencies_p"], random["latencies_p"], mob["latencies_p"]], axis=1)
    combined["hops_p"] = pd.concat([pietzuch["hops_p"], starks["hops_p"], random["hops_p"], mob["hops_p"]], axis=1)
    combined["load_p"] = pd.concat([pietzuch["load_p"], starks["load_p"], random["load_p"], mob["load_p"]], axis=1)

    # N = tn + fp
    # P = tp + fn
    # accuracy: (tp + tn) / (N + P) -> fraction of correct predictions
    # precision: tp / (tp + fp) -> fraction of correct "req holds" predictions
    # recall: tp/(tp + fn)      -> fraction of correctly predicted cases of "requirement holds"
    # cases:
    # tp: actual <= bounds && prediction <= bounds
    # fp: actual > bounds && prediction <= bounds
    # fn: actual <= bounds && prediction > bounds
    # tn: actual > bounds && prediction > bounds
    # prediction: "req holds"
    latency_acc = OrderedDict()
    latency_prec = OrderedDict()
    latency_rec = OrderedDict()
    hops_acc = OrderedDict()
    hops_prec = OrderedDict()
    hops_rec = OrderedDict()
    load_acc = OrderedDict()
    load_prec = OrderedDict()
    load_rec = OrderedDict()

    a,b,c = calculate_metrics(combined["latencies"], combined["latencies_p"])

    # plot the average acc/prec/rec over predictions from all runs (regardless of active algorithm) with stderror per req value
    ax = plt.errorbar(range(len(pietzuch_prec)), list(pietzuch_prec.values()), align='center')
    ax = plt.xticks(range(len(pietzuch_prec)), list(pietzuch_prec.keys()))



def plot_rq1_1():

    def plot_subfolder(f):
        generate_metric_plots_with_estimates(pietzuch, "pietzuch", f)
        generate_metric_plots_with_estimates(starks, "starks", f)
        generate_metric_plots_with_estimates(random, "random", f)
        generate_metric_plots_with_estimates(mob, "mob", f)
        generate_rel_error_plots(pietzuch, starks, mob, f)
        #bar_plot_metric({ "actual" : pietzuch["latencies"], "estimates" : pietzuch["latencies_p"]}, "latency")
        pietzuch_lat = pd.DataFrame([pietzuch["latencies"].mean(axis=1), (pietzuch["latencies_p"].mean(axis=1))]).T
        starks_lat = pd.DataFrame([starks["latencies"].mean(axis=1), (starks["latencies_p"].mean(axis=1))]).T
        mob_lat = pd.DataFrame([mob["latencies"].mean(axis=1), (mob["latencies_p"].mean(axis=1))]).T
        pietzuch_load = pd.DataFrame([pietzuch["load"].mean(axis=1), (pietzuch["load_p"].mean(axis=1))]).T
        starks_load = pd.DataFrame([starks["load"].mean(axis=1), (starks["load_p"].mean(axis=1))]).T
        mob_load = pd.DataFrame([mob["load"].mean(axis=1), (mob["load_p"].mean(axis=1))]).T
        pietzuch_hops = pd.DataFrame([pietzuch["hops"].mean(axis=1), (pietzuch["hops_p"].mean(axis=1))]).T
        starks_hops = pd.DataFrame([starks["hops"].mean(axis=1), (starks["hops_p"].mean(axis=1))]).T
        mob_hops = pd.DataFrame([mob["hops"].mean(axis=1), (mob["hops_p"].mean(axis=1))]).T

        bar_plot_metric(pietzuch_lat, "Relaxation", "Latency [ms]", f)
        bar_plot_metric(starks_lat, "Starks", "Latency [ms]", f)
        bar_plot_metric(mob_lat, "Mobility Tolerant", "Latency [ms]", f)
        bar_plot_metric(pietzuch_load, "Relaxation", "Load [cpu share]", f)
        bar_plot_metric(starks_load, "Starks", "Load [cpu share]", f)
        bar_plot_metric(mob_load, "Mobility Tolerant", "Load [cpu share]", f)
        bar_plot_metric(pietzuch_hops, "Relaxation", "Hops [#]", f)
        bar_plot_metric(starks_hops, "Starks", "Hops [#]", f)
        bar_plot_metric(mob_hops, "Mobility Tolerant", "Hops [#]", f)

    # avg of mobile an non-mobile sims
    pietzuch, starks, random, mob = load_data(folderdict["sim1"])
    plot_subfolder(folderdict["sim1"])
    # avg of non-mobile sims
    pietzuch, starks, random, mob = load_data(folderdict["sim1"]+"/no_mobility")
    plot_subfolder(folderdict["sim1"]+"/no_mobility")
    # avg of mobile sims
    pietzuch, starks, random, mob = load_data(folderdict["sim1"]+"/mobility")
    plot_subfolder(folderdict["sim1"]+"/mobility")
    #bar_plot_metric(rel_error)

    #generate_prediction_accuracy_errorbar_plots(pietzuch, starks, random, mob)

def plot_rq1_2(pietzuch, transitions, foldername):
    # note: files and folder are called pietzuch only because it is the starting algorithm
    pietzuch_lat = pd.DataFrame([pietzuch["latencies"].mean(axis=1)]).T
    pietzuch_load = pd.DataFrame([pietzuch["load"].mean(axis=1)]).T
    pietzuch_hops = pd.DataFrame([pietzuch["hops"].mean(axis=1)]).T
    bar_plot_metric(pietzuch_lat, "Relaxation_"+str(transitions), "Latency [ms]", foldername)
    bar_plot_metric(pietzuch_load, "Relaxation_"+str(transitions), "Load [cpu share]", foldername)
    bar_plot_metric(pietzuch_hops, "Relaxation_"+str(transitions), "Hops [#]", foldername)


# 3 dictionaries with data frames for each metric and metric prediction
folderdict = {"sim1": "/home/niels/Documents/BA-Niels/code/evaluation/sim1_filtered/20180625-16:14:36/logs_backup",
              "sim2": "/home/niels/Documents/BA-Niels/code/evaluation/sim2_filtered",
              "sim3": "/home/niels/Documents/BA-Niels/code/evaluation/sim3_filtered/20180627-05:04:28",
              "sim4": "/home/niels/Documents/BA-Niels/code/evaluation/sim4_filtered",
              "single" : "/home/niels/Documents/BA-Niels/code/evaluation/sim4_filtered/20180627-22:35:53/logs_backup/20180627-19:55:41"}


if sys.argv[1].lower() == "rq1_1":
    pietzuch, starks, random, mob = load_data(folderdict["sim1"])
    # barplot with actual and prediction per algorithm, metric plot with actual and prediction per algorithm
    print "plotting for rq1_1"
    plot_rq1_1()
    #cdf or accuracy plot over all simulations per req value

if sys.argv[1].lower() == "rq1_2":
    print "plotting for rq1_2"
    # combined random and context based latency plot
    pietzuch1, starks, random, mob = load_data(folderdict["sim2"])
    pietzuch2, starks, random, mob = load_data(folderdict["sim3"])
    plot_rq1_2(pietzuch1, "random_", folderdict["sim2"])
    plot_rq1_2(pietzuch2, "context-based_", folderdict["sim3"])
    plt.clf()
    plt = line_plot_metric_two_axes(pietzuch1["latencies"], pietzuch2["latencies"], stddev=False)
    plt.xlabel('Time [s]')
    plt.ylabel('Average Latency [ms]')
    plt.legend(['random transitions', 'context-based transitions'], loc='upper right')
    #plt.axvspan(300, 600, alpha=0.5, label = "Transition")
    plt.savefig(str(folderdict["sim3"])+"/"+"latency_avg_context_and_random")
    plt.savefig(str(folderdict["sim2"])+"/"+"latency_avg_context_and_random")
    #plt.show()
    plt.clf()

if sys.argv[1].lower() == "rq2":
    pietzuch, starks, random, mob = load_data(folderdict["sim4"])

    plt.clf()
    style.use('seaborn')

    ax = pietzuch["latencies"].mean(axis=1).plot()
    x = [200, 240]
    y = [0.0, 1800.0]
    #plt.plot(x, y, color='red', alpha=0.4)

    plt.xlabel('Time [s]')
    plt.ylabel('Average Latency [ms]')
    #plt.legend(['average latency', 'begin of mobility'], loc='upper left')
    plt.axvspan(275, 350, alpha=0.4, label = "Transition")
    plt.axvspan(695, 720, alpha=0.4, label = "Transition")
    plt.axvspan(200,240, alpha=0.4, facecolor="red", label = 'Begin of Mobility')

    plt.savefig(str(folderdict["sim4"])+"/"+"latency_avg_context-based_transitions")
    #plt.show()
    plt.clf()

    ax = pietzuch["load"].mean(axis=1).plot()
    x = [0, 600]
    y = [2.0, 2.0]
    plt.plot(x, y)
    x = [235, 240]
    y = [0.0, 3.0]
    #plt.plot(x, y)

    plt.xlabel('Time [s]')
    plt.ylabel('Average Load [cpu share]')
    plt.legend(['average load', 'requirement', 'begin of mobility'], loc='upper left')
    plt.axvspan(275, 350, alpha=0.4, label = "Transition")
    plt.axvspan(695, 720, alpha=0.4, label = "Transition")
    plt.axvspan(200,240, alpha=0.4, facecolor="red", label = 'Begin of Mobility')
    plt.savefig(str(folderdict["sim4"])+"/"+"load_avg_context-based_transitions")
    #plt.show()
    plt.clf()
