import argparse
import logging
import time

import joblib
import pandas as pd
from flask import Flask, jsonify, request, make_response
from river import linear_model, tree, ensemble, stream, metrics, preprocessing

app = Flask(__name__)

selected_features_throughput = ["Relaxation", "Rizou", "ProducerConsumer", "Random", "GlobalOptimalBDP", "MDCEP",
                                "operatorSelectivity", "eventRateIn", "processingLatencyMean", "processingLatencyStdDev"]
selected_features_processing_latency = ["Relaxation", "Rizou", "ProducerConsumer", "Random", "GlobalOptimalBDP", "MDCEP",
                                        "eventSizeInKB", "operatorSelectivity", "interArrivalMean", "interArrivalStdDev",
                                        "brokerCPULoad", "eventRateOut"]

selected_dict = { "eventRateOut" : selected_features_throughput, "processingLatencyMean" : selected_features_processing_latency}

@app.route('/updateOnline', methods=['POST'])
def update_online():
    if request.is_json:
        json_dict = request.get_json()
        sample = pd.DataFrame({feature: [val] for (feature, val) in json_dict})
        for target in targets:
            X = sample.drop(targets, axis=1)
            #X = X[online_features_throughput] if target == 'eventRateOut' else X # TODO
            y = sample[target]
            for x_i, y_i in stream.iter_pandas(X, y):
                for model in online_models[target]:
                    s = time.time()
                    # train on current sample
                    x_i_scaled = scaler.learn_one(x_i).transform_one(x_i)
                    model.learn_one(x_i_scaled, y_i)
                    logger.info("%s - time taken train: %5.4fs", model, float(time.time() - s))
        return {}, 202
    else:
        logger.error("expected json but got ", request)


@app.route('/predict', methods=['POST'])
def predict():
    start = time.time()
    if request.is_json:
        json_dict = request.get_json()
        logger.debug('=======\nreceived prediction request \n%s', json_dict)
        sample = pd.DataFrame({feature: [val] for (feature, val) in json_dict})
        online_predictions = predict_online_models(sample)
        sample_latency = sample[selected_dict['processingLatencyMean']]
        sample_throughput = sample[selected_dict['eventRateOut']]
        latency_prediction = trained_model_latency.predict(sample_latency)[0]
        throughput_prediction = trained_model_throughput.predict(sample_throughput)[0]
        # logger.info("prediction is %s", latency_prediction)
        predictions = {'offline': {'latency': latency_prediction, 'throughput': throughput_prediction},
                       'online': online_predictions }
        logger.debug("prediction took %4.3fs, reply is %s", float(time.time() - start), predictions)
        return make_response(jsonify(predictions), 200)
    else:
        logger.info("expected json but got ", request)


# predict is called multiple times with the same sample -> update separate
def predict_online_models(sample):
    predictions_dict = {target: {} for target in targets}
    for target in targets:
        X = sample[selected_dict[target]]
        y = sample[target]
        for x_i, y_i in stream.iter_pandas(X, y):
            for model in online_models[target]:
                s = time.time()
                # predict on current sample
                x_i_scaled = scaler.transform_one(x_i)
                pred = model.predict_one(x_i_scaled)
                p = time.time()
                predictions_dict[target][str(type(model))] = pred
                logger.info("%s: %s -> %8.1f (truth: %8.3f) - time taken predict: %5.4fs", target, model, float(pred), float(y), float(p - s))

    return predictions_dict


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--latencyModel', help='joblib file with the trained pipeline object for latency')
    parser.add_argument('-t', '--throughputModel', help='joblib file with the trained pipeline object for throughput')
    args = parser.parse_args()
    trained_model_latency = joblib.load(args.latencyModel)
    trained_model_throughput = joblib.load(args.throughputModel)

    scaler = preprocessing.StandardScaler()  # share the scaler for all models
    online_models_base = [
        linear_model.LinearRegression(),
        linear_model.PARegressor(),
        tree.HoeffdingTreeRegressor(),
        tree.HoeffdingAdaptiveTreeRegressor(),  # +ADWIN concept-drift detector
        ensemble.AdaptiveRandomForestRegressor(),
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


    # logging setup
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    file = logging.FileHandler("logs/predictionEndpoint.log")
    file.setLevel(logging.INFO)
    fileformat = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s", datefmt="%H:%M:%S")
    file.setFormatter(fileformat)
    logger.addHandler(file)
    console_log = logging.StreamHandler()
    console_log.setLevel(logging.INFO)
    logger.addHandler(console_log)
    logger.info("starting Flask app")
    # logging.basicConfig(filename='predictionEndpoint.log', level=logging.DEBUG)
    app.run(host='0.0.0.0', port=9091)
