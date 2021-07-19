FROM mfeurer/auto-sklearn:master

WORKDIR /app
RUN pip3 install pandas flask joblib river h2o==3.32.1.3
RUN apt update && apt install -y openjdk-11-jre-headless
RUN mkdir /app/logs
#RUN pip3 install --force-reinstall scipy==1.6.0 # needed for auto-sklearn model loading
COPY autosklearn_trained_pipeline_8_yahoo_geni_5s_combined_samples.csv_processingLatencyMean.joblib .
COPY autosklearn_trained_pipeline_8_yahoo_geni_5s_combined_samples.csv_eventRateOut.joblib .
COPY run_prediction_endpoint.py .

CMD python3 run_prediction_endpoint.py \
    -l autosklearn_trained_pipeline_8_yahoo_geni_5s_combined_samples.csv_processingLatencyMean.joblib \
    -t autosklearn_trained_pipeline_8_yahoo_geni_5s_combined_samples.csv_eventRateOut.joblib > /app/logs/predictionServerConsole.log 2>&1
