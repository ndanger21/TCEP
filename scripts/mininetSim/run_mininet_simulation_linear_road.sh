#!/usr/bin/env bash

work_dir="$(cd "$(dirname "$0")" ; pwd -P)"
source "$work_dir/docker-swarm-maki.cfg"
[ -f ${work_dir}/docker-swarm-maki.cfg ] || echo ".cfg file not found"
# Description: Runs a single containernet simulation with the specified settings
simDuration=$1
algorithm=$2
baseLatency=$3
mobility=$4
nPublishers=$5
combinedPIM=$6

# setup + initalization
echo "setting up mininet simulation with $algorithm as initial placement algorithm"

echo "stopping all other containers"
docker rm -f $(docker ps -a -q --filter ancestor=$registry_user/$containernet_image)
docker rm -f $(docker ps -a -q --filter ancestor=$registry_user/$tcep_image)
docker rm -f $(docker ps -a -q --filter ancestor=cloudwattfr/ntpserver)

if [ -d ~/logs ]; then
        foldername="logs_backup"
        if [ ! -d ~/$foldername ]; then
            mkdir -p  ~/"$foldername"
        fi
        echo "backing up previous logs"
        cp -rf ~/logs/ ~/$foldername/$(date +%Y%m%d-%H:%M:%S)
        #cp -rf ~/logs/ ~/logs_backup/$(date +%Y%m%d-%H:%M:%S)
        sudo rm -rf ~/logs/
else
# separate folders for each node and publisher -> debugging
    mkdir ~/logs
    mkdir ~/logs/simulation
    mkdir ~/logs/n0
    mkdir ~/logs/s0
    mkdir ~/logs/n1
    mkdir ~/logs/n2
    mkdir ~/logs/n3
    mkdir ~/logs/n4
    mkdir ~/logs/n5
    mkdir ~/logs/n6
    mkdir ~/logs/p1
    mkdir ~/logs/p2
    mkdir ~/logs/p3
    mkdir ~/logs/p4
    mkdir ~/logs/p5
    mkdir ~/logs/p6
    mkdir ~/logs/p7
    mkdir ~/logs/p8
    mkdir ~/logs/p9
    mkdir ~/logs/p10
    mkdir ~/logs/p11
    mkdir ~/logs/p12
fi

touch ~/tcep/handovers.csv
chmod a+w ~/tcep/handovers.csv
truncate -s 0 ~/tcep/handovers.csv


if [[ $run_local == 'false' ]]; then
  echo "pulling $registry_user/$tcep_image from dockerhub..."
  docker pull $registry_user/$tcep_image
  echo "\n pulling custom containernet image $registry_user/$containernet_image"
  docker pull $registry_user/$containernet_image
fi
echo "starting ntp container"
docker run --name nserver -d -p 123:123  cloudwattfr/ntpserver:latest

#############################################################################
# set up containernet, start simulation with tree network topology
#############################################################################
# mount /var/run/docker.sock so containers created by the simulation are run on the host
# and not inside the containernet container

res=`docker run --name containernet -d --rm --privileged \
    --pid='host' \
    --volume /var/run/docker.sock:/var/run/docker.sock \
    --volume ~/tcep:/containernet/tcep \
    $registry_user/$containernet_image \
    nohup python3 mininet_linear_road.py ${simDuration} ${algorithm} ${mobility} ${baseLatency} $USER $registry_user ${nPublishers} ${combinedPIM}`

echo "$(date +%H:%M:%S) created Containernet container $res, executing simulation..."
#docker run --name containernet -it --rm --privileged \
#    --pid='host' \
#    --volume /var/run/docker.sock:/var/run/docker.sock \
#    --volume ~/tcep:/containernet/tcep \
#    nzumbe/mycontainernet:latest \
#    python mininet_linear_road.py





#test
