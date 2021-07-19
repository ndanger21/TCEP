import argparse
import ast
import joblib
import logging
import pandas as pd
import time
from flask import Flask, jsonify, request, make_response
from river import linear_model, tree, stream, metrics, preprocessing

app = Flask(__name__)


@app.route('/predictBatch', methods=['POST'])
def predict_batch():
    start = time.time()
    if request.is_json:
        json_dict = request.get_json()
        op_sample_df = json_batch_to_df(json_dict)
        logger.info("sample data frame: \n%s" % op_sample_df)
        online_start = time.time()
        online_predictions = predict_online_models(op_sample_df)
        online_end = time.time()

        sample_latency = op_sample_df[selected_dict['processingLatencyMean']]
        sample_throughput = op_sample_df[selected_dict['eventRateOut']]
        if latency_is_h2o:
            sample_latency = h2o.H2OFrame(sample_latency)
        latency_prediction = trained_model_latency.predict(sample_latency)
        latency_end = time.time()
        if throughput_is_h2o:
            sample_throughput = h2o.H2OFrame(sample_throughput)
        throughput_prediction = trained_model_throughput.predict(sample_throughput)

        if latency_is_h2o:
            latency_prediction = latency_prediction.as_data_frame()["predict"].tolist()
        if throughput_is_h2o:
            throughput_prediction = throughput_prediction.as_data_frame()["predict"].tolist()

        offline_latencies = {}
        offline_throughputs = {}
        for i in range(0, int(len(json_dict))):
            offline_latencies[i] = float(latency_prediction[i])
            offline_throughputs[i] = float(throughput_prediction[i])

        # offline: Map(latency -> Map(op1 -> 0.0, op2 ->), throughput -> Map(...))
        # online: metric -> algo -> ops -> values
        #case class BatchPredictionResponse(offline: Map[String, Map[Long, Double]], online: Map[String, Map[String, Map[Long, Double]]])
        logger.info("latency is %s", latency_prediction)
        logger.info("throughput is %s", throughput_prediction)
        logger.info("online predictions are \n", online_predictions)
        predictions = {'offline': {'latency': offline_latencies, 'throughput': offline_throughputs},
                       'online': online_predictions }
        logger.info("json processing took %4.3fs online prediction took %4.3fs latency_offline took %4.3fs and throughput_offline took %4.3fs, total took %4.3fs",
                    float(online_start - start), float(online_end - online_start), float(latency_end - online_end), float(time.time() - latency_end), float(time.time() - start))
        logger.info("predictions are \n%s" % predictions)
        return make_response(jsonify(predictions), 200)
    else:
        logger.info("expected json but got ", request)


def json_batch_to_df(json_dict):
    logger.debug('=======\nreceived prediction request \n%s', json_dict)
    op_sample_df = pd.DataFrame()
    for op_id in json_dict:
        fv_list = ast.literal_eval(json_dict[op_id])
        s = pd.DataFrame({feature: [val] for (feature, val) in fv_list})
        s.set_index(pd.Index([op_id]), inplace=True)
        op_sample_df = op_sample_df.append(s)
    return op_sample_df


@app.route('/predict', methods=['POST'])
def predict():
    start = time.time()
    if request.is_json:
        json_dict = request.get_json()
        logger.debug('=======\nreceived prediction request \n%s', json_dict)
        sample_dict = {feature: [val] for (feature, val) in json_dict}
        logger.info(sample_dict)
        op_sample_dict = { op: { feature: [val] for (feature, val) in sample } for (op, sample) in json_dict}
        logger.debug("\nop-sample dict:\n", op_sample_dict)
        # Map(op1 -> sample1, op2 -> sample2, ...)
        #data =
        # row1: op1 -> sample1V1, sample1V2
        # row2: op2 -> sample2V1, sample2V2
        #pd.DataFrame.from_dict(data, orient='index')
        sample = pd.DataFrame({feature: [val] for (feature, val) in json_dict})
        logger.debug("sample data frame: \n%s" % sample)
        online_start = time.time()
        online_predictions = predict_online_models(sample)
        online_end = time.time()
        sample_latency = sample[selected_dict['processingLatencyMean']]
        sample_throughput = sample[selected_dict['eventRateOut']]
        if latency_is_h2o:
            sample_latency = h2o.H2OFrame(sample_latency)
        latency_prediction = trained_model_latency.predict(sample_latency)[0]
        latency_end = time.time()
        if throughput_is_h2o:
            sample_throughput = h2o.H2OFrame(sample_throughput)
        throughput_prediction = trained_model_throughput.predict(sample_throughput)[0]

        if latency_is_h2o:
            latency_prediction = latency_prediction.getrow()[0]
        if throughput_is_h2o:
            throughput_prediction = throughput_prediction.getrow()[0]
        logger.debug("prediction is %s", latency_prediction)
        logger.debug("throughput is %s", throughput_prediction)
        predictions = {'offline': {'latency': latency_prediction, 'throughput': throughput_prediction},
                       'online': online_predictions }
        logger.info("json processing took %4.3fs online prediction took %4.3fs latency_offline took %4.3fs and throughput_offline took %4.3fs, total took %4.3fs",
                    float(online_start - start), float(online_end - online_start), float(latency_end - online_end), float(time.time() - latency_end), float(time.time() - start))
        #logger.info("prediction took %4.3fs, reply is %s", float(time.time() - start), predictions)
        return make_response(jsonify(predictions), 200)
    else:
        logger.info("expected json but got ", request)


@app.route('/updateOnlineBatch', methods=['POST'])
def update_online():
    if request.is_json:
        json_dict = request.get_json()
        op_sample_df = json_batch_to_df(json_dict)

        for target in targets:
            X = op_sample_df[selected_dict[target]]  # sample.drop(targets, axis=1)
            y = op_sample_df[target]
            for x_i, y_i in stream.iter_pandas(X, y):
                for model in online_models[target]:
                    s = time.time()
                    # train on current sample
                    #x_i_scaled = scaler.learn_one(x_i).transform_one(x_i)
                    model.learn_one(x_i, y_i)
                    logger.debug("%s - time taken train: %5.4fs", model, float(time.time() - s))
        return {}, 202
    else:
        logger.error("expected json but got ", request)


# predict is called multiple times with the same sample -> update separate
def predict_online_models(sample):
    predictions_dict = {target: {} for target in targets}
    logger.debug("starting online predictions")
    for target in targets:
        X = sample[selected_dict[target]]
        y = sample[target]
        logger.debug("X, Y are \n%s | %s" % (X, y))
        for model in online_models[target]:
            predictions_dict[target][str(model.steps)] = {}
            i = 0
            for x_i, y_i in stream.iter_pandas(X, y):
                s = time.time()
                # predict on current sample
                #x_i_scaled = scaler.transform_one(x_i)
                pred = model.predict_one(x_i)
                p = time.time()
                predictions_dict[target][str(model.steps)][i] = float(pred)
                predict_time = float(p - s)
                logger.debug("online prediction for %i is %s" % (i, pred))
                logger.info("%s: %s - time taken predict: %5.4fs", target, model, predict_time) if predict_time > 0.005 else None
                i += 1

    return predictions_dict



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--latencyModel', help='joblib file with the trained pipeline object for latency')
    parser.add_argument('-t', '--throughputModel', help='joblib file with the trained pipeline object for throughput')
    args = parser.parse_args()
    # logging setup
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    #file = logging.FileHandler("logs/predictionEndpoint.log")
    #file.setLevel(logging.INFO)
    #fileformat = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s", datefmt="%H:%M:%S")
    #file.setFormatter(fileformat)
    #logger.addHandler(file)
    console_log = logging.StreamHandler()
    console_log.setLevel(logging.INFO)
    logger.addHandler(console_log)
    logger.info("starting Flask app")
    logger.info("loading models: \n latency: %s \n throughput: %s " %
                (args.latencyModel, args.throughputModel))

    trained_model_throughput = None
    trained_model_latency = None
    throughput_is_h2o = "h2o" in args.throughputModel
    latency_is_h2o = "h2o" in args.latencyModel
    if throughput_is_h2o or latency_is_h2o:
        import h2o
        h2o.init()

        if throughput_is_h2o:
            trained_model_throughput = h2o.load_model(args.throughputModel)

        if latency_is_h2o:
            trained_model_latency = h2o.load_model(args.latencyModel)

    if trained_model_throughput is None:
        trained_model_throughput = joblib.load(args.throughputModel)
    if trained_model_latency is None:
        trained_model_latency = joblib.load(args.latencyModel)


    online_models_base = [
        preprocessing.StandardScaler() | linear_model.LinearRegression(),
        preprocessing.StandardScaler() | linear_model.PARegressor(),
        preprocessing.StandardScaler() | tree.HoeffdingTreeRegressor(splitter=tree.splitter.QOSplitter()),
        preprocessing.StandardScaler() | tree.HoeffdingAdaptiveTreeRegressor(splitter=tree.splitter.QOSplitter()),  # +ADWIN concept-drift detector
        #preprocessing.StandardScaler() | ensemble.AdaptiveRandomForestRegressor(splitter=tree.splitter.QOSplitter()) #disabled since too slow at predicting;
        #neural_net.MLPRegressor(
        #  hidden_dims=(5,),
        #  activations=(
        #      activations.ReLU,
        #      activations.ReLU,
        #      activations.Identity
        #  ))
        ]
    metric = metrics.RMSE()

    targets = ['processingLatencyMean', 'eventRateOut']
    online_models = {name: online_models_base.copy() for name in targets}


    selected_features_throughput = ["Relaxation", "Rizou", "ProducerConsumer", "Random", "GlobalOptimalBDP", "MDCEP",
                                    "operatorSelectivity", "eventRateIn", "processingLatencyMean", "processingLatencyStdDev", "brokerCPULoad", "networkParentLatencyMean"]
    if "yahoo" in args.throughputModel:
        selected_features_throughput.remove("brokerCPULoad")
        selected_features_throughput.remove("networkParentLatencyMean")

    if "linearRoad" in args.throughputModel or (("accident" in args.throughputModel) and ("autosklearn" in args.throughputModel)):
        # slightly different order
        selected_features_throughput = ['Relaxation', 'Rizou', 'ProducerConsumer', 'Random', 'GlobalOptimalBDP', 'MDCEP',
                                        'operatorSelectivity', 'eventRateIn', 'processingLatencyMean', 'processingLatencyStdDev', 'networkParentLatencyMean', 'brokerCPULoad']

    selected_features_processing_latency = ["Relaxation", "Rizou", "ProducerConsumer", "Random", "GlobalOptimalBDP", "MDCEP",
                                            "eventSizeInKB", "operatorSelectivity", "interArrivalMean", "interArrivalStdDev",
                                            "brokerCPULoad", "eventRateOut"]

    selected_dict = { "eventRateOut": selected_features_throughput, "processingLatencyMean": selected_features_processing_latency}



    # logging.basicConfig(filename='predictionEndpoint.log', level=logging.DEBUG)
    app.run(host='0.0.0.0', port=9091)
