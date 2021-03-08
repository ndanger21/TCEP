#!/usr/bin/env bash
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../.."
#work_dir="/home/$USER"
source ${work_dir}/docker-swarm.cfg

printf "\n building custom containernet docker image "
cd $work_dir/scripts/mininetSim/mycontainernet \
&& docker build -t $registry_user/$containernet_image . \
&& [[ $run_local == 'false' ]] && docker push $registry_user/$containernet_image
echo "built containernet image"
