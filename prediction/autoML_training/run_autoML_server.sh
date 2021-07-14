#!/bin/bash
host=$1
user=$2

if [ -z $host ]; then
  host="localhost"
fi
if [ -z $user ]; then
  user=$USER
fi
ssh ${user}@${host} "mkdir ~/automl"
ssh ${user}@${host} "mkdir ~/automl/output"
scp ../*.ipynb ${user}@${host}:~/automl/
#scp Dockerfile ${user}@${host}:~/automl_server/
#ssh ${user}@${host} "cd ~/automl_server && docker build -t tcep-automl-server:latest ."
ssh ${user}@${host} 'docker run -d -v $PWD:/opt/nb --name tcep-automl-server -p 8888:8888 nieda2018/tcep-automl-server /bin/bash -c "mkdir -p /opt/nb && jupyter notebook --notebook-dir=/opt/nb --ip='0.0.0.0' --port=8888 --no-browser --allow-root"'
ssh ${user}@${host} "docker exec tcep-automl-server jupyter notebook list"
echo "access jupyter notebook on $host with above token"
