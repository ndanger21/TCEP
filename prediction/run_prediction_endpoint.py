import argparse
import joblib
import pandas as pd
import time
from flask import Flask, jsonify, request, make_response

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    start = time.time()
    if request.is_json:
        json_dict = request.get_json()
        print('received prediction request \n', json_dict)
        #sample = pd.read_json(json, orient='records')
        sample = pd.DataFrame({feature: [val] for (feature, val) in json_dict.items()})
        print(sample)
        latency_prediction = trained_model_latency.predict(sample)[0]
        throughput_prediction = trained_model_throughput.predict(sample)[0]
        print("prediction is ", latency_prediction)
        predictions = {'latency': latency_prediction, 'throughput': throughput_prediction}
        end = time.time()
        print("call took ", end - start)
        print("reply is ", jsonify(predictions))
        return make_response(jsonify(predictions), 200)
    else:
        print("expected json but got ", request)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--latencyModel', help='joblib file with the trained pipeline object for latency')
    parser.add_argument('-t', '--throughputModel', help='joblib file with the trained pipeline object for throughput')
    args = parser.parse_args()
    trained_model_latency = joblib.load(args.latencyModel)
    trained_model_throughput = joblib.load(args.throughputModel)
    app.run(host='0.0.0.0', port=9091)


