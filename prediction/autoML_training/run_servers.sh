#!/bin/bash
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../.."
source "$work_dir/docker-swarm_maki.cfg"
CMD=$1
manager="10.0.30.15"
workers=("10.2.1.40" "10.2.1.74" "10.2.1.84" "10.2.1.92" "10.0.30.10")

build_and_start() {
  echo "starting automl jupyter server on $manager"
  bash run_autoML_server.sh $manager &
  for i in "${workers[@]}"
  do
    echo "starting automl jupyter server on $i"
    bash run_autoML_server.sh $i
  done
  res=`wait`
  echo "setup done"
}

copy_files() {
  ssh ${user}@${manager} "mkdir ~/automl"
  scp ../tpot.ipynb ../h2o.ipynb ../autosklearn.ipynb ${user}@${manager}:~/automl/
  scp ../yahoo_geni_15s_combined_samples.csv ${manager}@${i}:~/automl/

  for i in "${workers[@]}"
  do
    ssh ${user}@${i} "mkdir ~/automl"
    scp ../tpot.ipynb ../h2o.ipynb ../autosklearn.ipynb ${user}@${i}:~/automl/
    scp ../yahoo_geni_15s_combined_samples.csv ${user}@${i}:~/automl/
  done

}

print_tokens() {
  echo "checking $manager"
  ssh ${user}@${manager} "docker exec tcep-automl-server jupyter notebook list" && \
  echo "access jupyter notebook on $manager with above token"
  for i in "${workers[@]}"
  do
    echo "checking $i"
    ssh ${user}@${i} "docker exec tcep-automl-server jupyter notebook list" && \
    echo "access jupyter notebook on $i with above token"
  done

}


echo "running $CMD"
$CMD
