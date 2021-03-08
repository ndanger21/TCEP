#!/usr/bin/env bash

work_dir="$(cd "$(dirname "$0")" ; pwd -P)"
source "$work_dir/tcep/docker-swarm-maki.cfg"
[ -f ${work_dir}/tcep/docker-swarm-maki.cfg ] || echo ".cfg file not found"
# Description: Runs a single containernet simulation with the specified settings
simDuration=$1
algorithm=$2
baseLatency=$3
mobility=$4
nPublishers=$5
nPublishersPerSection=$6

# setup + initalization
echo "setting up mininet simulation with $algorithm as initial placement algorithm"

echo "stopping all other containers"
docker rm -f $(docker ps -a -q --filter ancestor=$registry_user/$containernet_image)
docker rm -f $(docker ps -a -q --filter ancestor=$registry_user/$tcep_image)
docker rm -f $(docker ps -a -q --filter ancestor=$registry_user/$gui_image)
docker rm -f $(docker ps -a -q --filter ancestor=cloudwattfr/ntpserver)

if [ -d ~/logs ]; then
        foldername="logs_backup"
        if [ ! -d ~/$foldername ]; then
            mkdir -p  ~/"$foldername"
        fi
        echo "backing up previous logs"
        cp -rf ~/logs/ ~/$foldername/$(date +%Y%m%d-%H:%M:%S)
        rm -rf ~/logs/
else
# separate folders for each node and publisher -> debugging
    mkdir ~/logs
    mkdir ~/logs/simulation
fi

touch ~/tcep/handovers.csv
chmod a+w ~/tcep/handovers.csv
truncate -s 0 ~/tcep/handovers.csv


if [[ $run_local == 'false' ]]; then
  echo "pulling $registry_user/$tcep_image from dockerhub..."
  docker pull $registry_user/$tcep_image
  echo "\n pulling custom containernet image $registry_user/$containernet_image"
  docker pull $registry_user/$containernet_image
  echo "pulling $registry_user/$gui_image from dockerhub..."
  docker pull $registry_user/$gui_image
fi
echo "starting ntp container"
docker run --name nserver -d -p 123:123  cloudwattfr/ntpserver:latest

#############################################################################
# set up containernet, start simulation with tree network topology
#############################################################################
# mount /var/run/docker.sock so containers created by the simulation are run on the host
# and not inside the containernet container
echo "args: $simDuration $algorithm $mobility $baseLatency $USER $registry_user $nPublishers $nPublishersPerSection"
res=`docker run --name containernet -d --rm --privileged \
    --pid='host' \
    --volume /var/run/docker.sock:/var/run/docker.sock \
    --volume ~/tcep:/containernet/tcep \
    $registry_user/$containernet_image \
    python3 mininet_wired_tree.py ${simDuration} ${algorithm} ${mobility} ${baseLatency} $HOME $registry_user ${nPublishers} ${nPublishersPerSection} > nohup.out`

echo "$(date +%H:%M:%S) created Containernet container $res, executing simulation..."

#docker run --name containernet -it --rm --privileged \
#    --pid='host' \
#    --volume /var/run/docker.sock:/var/run/docker.sock \
#    --volume ~/tcep:/containernet/tcep \
#    nieda2018/mycontainernet:latest \
#    python3 mininet_wired_tree.py 10 "MDCEP" "True" 10 $HOME "nieda2018" 8 2