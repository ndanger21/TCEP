#!/bin/bash
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../.."
source "$work_dir/docker-swarm_maki.cfg"
CMD=$1
target_framework=$2
workers+=($manager)
#workers=("10.2.1.40" "10.2.1.64" "10.2.1.72" "10.2.1.74" "10.2.1.84" "10.2.1.92" "10.0.30.15")
workers=("10.2.1.40" "10.2.1.72" "10.2.1.74" "10.2.1.84" "10.2.1.92" "10.0.30.15")

build_and_start() {
  for i in "${workers[@]}"
  do
    echo "starting automl jupyter server on $i"
    bash run_autoML_server.sh $i
  done
  res=`wait`
  echo "setup done"
}

copy_files() {
  #get_output
  for i in "${workers[@]}"
  do
    ssh ${user}@${i} "mkdir ~/automl"
    scp ../tpot*.ipynb ../h2o*.ipynb ../autosklearn*.ipynb ${user}@${i}:~/automl/
    scp ../mininet-wifi_linearRoad_15s_combined_samples.csv ${user}@${i}:~/automl/
  done

}

print_tokens() {
  for i in "${workers[@]}"
  do
    echo "checking $i"
    ssh ${user}@${i} "docker exec tcep-automl-server jupyter notebook list" && \
    echo "access jupyter notebook on $i with above token"
  done

}

get_output() {
  echo $workers[@]
  for i in "${workers[@]}"
  do
    echo "fetching results from $i"
    datestr=$(date '+%d-%b-%Y-%H-%M-%S')
    mkdir -p ~/Downloads/automl/$i/$datestr
    ssh ${user}@${i} "mkdir -p ~/automl_bkp/$datestr"
    ssh ${user}@${i} "cp -r ~/automl/* ~/automl_bkp/$datestr"
    scp ${user}@${i}:~/automl/*.ipynb ~/Downloads/automl/$i/$datestr/
    scp ${user}@${i}:~/automl/*.scores.csv ~/Downloads/automl/$i/$datestr/
    scp -r ${user}@${i}:~/automl/output/ ~/Downloads/automl/$i/$datestr/
    ssh -t ${user}@${i} "rm -rf ~/automl/output/*"
  done
}


echo "running $CMD"
$CMD
