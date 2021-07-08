FROM mfeurer/auto-sklearn:master

RUN pip3 install tpot torch h2o xgboost pandas joblib sklearn verstack
RUN pip3 install matplotlib
RUN apt update && apt install -y openjdk-11-jre-headless
