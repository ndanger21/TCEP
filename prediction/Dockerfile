FROM mfeurer/auto-sklearn:master

WORKDIR /app
RUN pip3 install tpot pandas flask joblib river
RUN mkdir /app/logs
RUN pip3 install --force-reinstall scipy==1.6 # needed for auto-sklearn model loading
COPY run_prediction_endpoint.py .
COPY tpot_trained_pipeline.joblib .
# TODO include trained models
CMD python3 run_prediction_endpoint.py -l tpot_trained_pipeline.joblib -t tpot_trained_pipeline.joblib
