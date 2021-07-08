#!/usr/bin/env bash
# Author: Manisha Luthra
# Modified by: Sebastian Hennig
# Description: Sets up and execute TCEP on GENI testbed


work_dir="$(cd "$(dirname "$0")" ; pwd -P)/.."

CMD="$1"
shift
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -u|--user)
    u="$2"
    shift
    shift
    ;;
    -h|--host)
    host="$2"
    shift
    shift
    ;;
    -s|--stack-file)
    stack_file="$2"
    shift
    shift
    ;;
    -c|--cfg)
    config_file="$2"
    shift
    shift
    ;;
    -l|--location)
    location="$2"
    shift
    shift
    ;;
esac
done

if [ -z $location ]; then
  if [ -z $config_file ]; then
    config_file="/docker-swarm_local.cfg"
  fi
  if [ -z $stack_file ]; then
    stack_file="docker-stack_local.yml"
  fi
else
  config_file="/docker-swarm_$2.cfg"
  stack_file="docker-stack_$2.yml"
fi

source "$work_dir/$config_file"
echo "docker stack file is $work_dir$stack_file"
echo "cfg file is $work_dir$config_file"
source "$work_dir/scripts/common_functions.sh"
if [ -z $u ]; then
  u=user
fi
if [ -z $host ]; then
  host=$manager
fi

if [[ ${host} == "localhost" ]]; then
  local_run=true
else
  local_run=false
fi
token="undefined"
maki_pat='^[10]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
geni_pat='^[72,130]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
cl_pat='^[128]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}'
u=""

setUser() {

#if [[ $1 =~ $maki_pat ]]; then
#	u=$maki_user
#elif [[ $1 =~ $geni_pat ]]; then
#	u=$geni_user
#else
#	u=$cl_user
#fi
  u=$user
}

all() {
    echo "running TCEP simulation with manager $host and workers $workers"
    $local_run && echo "local_run $local_run"
    #setup
    publish_with_swarm_restart
}

publish_with_swarm_restart() {
    START=$(date +%s.%N)
    take_down_swarm
    swarm_token=$(init_manager) && \
    join_workers $swarm_token && \
    PUB_START=$(date +%s.%N) && \
    publish && \
    END=$(date +%s.%N) && \
    TOT_DIFF=$(echo "$END - $START" | bc) && \
    echo "total time diff is $TOT_DIFF" && \
    PUB_DIFF=$(echo "$END - $PUB_START" | bc) && \
    echo "publish time diff is $PUB_DIFF"
}

adjust_cfg() {
    local sections=$n_speed_streams
    if $local_run; then
      local nNodesTotal=7
    else
      local nNodesTotal=$((${#workers[@]}+1))
      echo "total nodes: $nNodesTotal, (speed) streams: $n_speed_streams"
    fi
    # numberOfMininetMobilitySections nSpeedPublishers nNodesTotal GUIHost mininetSimulation mininetWifiSimulation
    adjust_config $sections $sections $nNodesTotal $host "false" "false"
}

take_down_swarm() {
    setUser $manager
    echo "Taking down possible existing swarm with manager $manager"
    ssh -Tq -p $port $u@$manager 'docker stack rm tcep; docker swarm leave --force && docker network prune -f || exit 0;'
    $local_run || for i in "${workers[@]}"
    do
	      setUser $i
	      echo "$i leaving swarm"
        ssh -Tq -p $port $u@$i 'docker swarm leave --force && docker network prune -f || exit 0;'
    done
}



setup() {
  if [ ${#workers[@]} -le $((n_publisher_nodes_total)) ]; then
      echo "not enough non-manager nodes (currently ${#workers[@]}) for ${n_publisher_nodes_total} publishers and at least 1 worker app" && exit 1
  else
    echo "$(date +%H:%M:%S) setting up manager $manager and workers $workers"
    count=0
    ${local_run} || for i in "${workers[@]}"
    do
      count=$((count+1))
      setUser $i
      setup_docker $i $u $count #> /dev/null
    done
    setUser $manager
    setup_docker $manager $u 0
    res=`wait` # wait for all setup_instance calls to finish
    echo "$(date +%H:%M:%S) setup done"
  fi
}

init_manager() {
    
     setUser $manager
    `ssh -T -p $port $u@$manager "sudo hostname node0; sudo systemctl restart docker"` 2> /dev/null
    if $local_run; then
      docker swarm init
    else
      `ssh -T -p $port $u@$manager "docker swarm init --advertise-addr $manager"` 2> /dev/null
    fi
    # currently we are pushing jar file to all of the workers and building image locally on all of the workers
    # in future we should use docker registry service to create image on manager and use it as reference in worker nodes
    #ssh $user@$manager "docker service create --name registry --publish published=5000,target=5000 registry:2"
    token=`ssh -T -p $port $u@$manager 'docker swarm join-token worker -q'`
    echo $token #using this a global variable (DONT REMOVE)
}

join_workers() {
    count=0
    ${local_run} || for i in "${workers[@]}"
    do
      ssh ${u}@${i} "mkdir -p ~/tcep/event_traces"
      setUser $i
	    count=$((count+1))
      ssh $u@$i "echo 'processing worker $i'"
      ssh -T -p $port $u@$i "docker swarm join --token $1 $manager:2377"
      #ssh -T -p $port $u@$i "sudo systemctl restart docker" 2> /dev/null
      # add labels to docker nodes so replicas can be deployed according to label
      if [ "$count" -le "$n_publisher_nodes_total" ]; then
        ssh -T -p $port $user@$manager "docker node update --label-add publisher=true node$count"
        ssh -T -p $port $user@$manager "docker node update --label-add worker=false node$count"
        echo "added label publisher=true to node$count "
      else
        ssh -T -p $port $user@$manager "docker node update --label-add worker=true node$count"
        ssh -T -p $port $user@$manager "docker node update --label-add publisher=false node$count"
        echo "added label worker=true to node$count "
      fi
    done
    ${local_run} && docker node update --label-add worker=true node0 && docker node update --label-add publisher=true node0
    ssh -T -p $port $u@$manager "docker node update --label-add subscriber=true node0"
    echo "added label subscriber=true to node0"
    ssh ${u}@${manager} "mkdir -p ~/tcep/event_traces"
}

rebootSwarm() {
    ##take_down_swarm
    for i in "${workers[@]}"
    do
	      setUser $i
        echo "rebooting worker $i"
        ssh -p $port $u@$i "sudo reboot" &>/dev/null
    done
    echo "rebooting manager"
    setUser $manager
    ssh -p $port $u@$manager "sudo reboot" &>/dev/null
}

# get the output from the manager node
# Usage bash publish_tcep.sh getOutput
get_output(){
	setUser $manager
	ssh $u@$manager "mkdir -p ~/logs_backup/$(date '+%d-%b-%Y-%H-%M-%S')"
	ssh $u@$manager "cd ~/logs && zip -r -o traces_$(date '+%d-%b-%Y-%H-%M-%S').zip *"
	ssh $u@$manager "cp -r ~/logs/*.zip ~/logs_backup/"
	mkdir -p $work_dir/logs_backup/$(date '+%d-%b-%Y-%H-%M-%S')
	scp -r $u@$manager:~/logs/*.zip $work_dir/logs_backup/$(date '+%d-%b-%Y-%H-%M-%S')
}

clear_logs() {
    setUser $manager
    ssh $u@$manager "rm -f ~/logs/*.log && rm -f ~/logs/*.csv && rm -f ~/logs/*.zip" &
    for i in "${workers[@]}"
    do
	    setUser $i
      ssh $u@$i "rm -f ~/logs/*.log && rm -f ~/logs/*.csv" &
    done

}

build_image() {
    adjust_cfg && \
    bash $work_dir"/scripts/build.sh" "localhost" ${config_file}
}

publish_no_build() {
    get_output
    clear_logs
    setUser $manager
    printf "\nPulling image from registry\n"
    ssh -T -p $port $user@$manager "docker pull $registry_user/$tcep_image"
    ssh -T -p $port $user@$manager "docker pull $registry_user/$gui_image"
    ssh -T -p $port $user@$manager "docker pull $registry_user/tcep-prediction-endpoint"

    # stop already existing services
    #ssh $user@$manager 'docker service rm $(docker service ls -q)'
    ssh $user@$manager 'docker stack rm tcep'
    sleep 10s  # wait a bit so stack network is removed properly
    #ssh $user@$manager 'docker network prune'

    echo "Booting up new stack"
    IFS='/' read -ra my_array <<< ${stack_file}
    stack_file_name=${my_array[-1]} #TODO fix this
    echo "stack_file name: $stack_file_name"
    ssh -p $port $u@$manager 'mkdir -p ~/logs && rm -f ~/logs/** && mkdir -p ~/src';
    scp -P $port $work_dir/${stack_file} $u@$manager:~/src/${stack_file_name}
    #ssh -p $port $u@$manager 'cd ~/src && docker stack deploy --prune --with-registry-auth -c docker-stack.yml tcep';
    ssh -p $port $u@$manager 'cd ~/src && docker stack deploy --with-registry-auth -c '${stack_file_name}' tcep';
    #clear_logs
}

publish() {
  build_image && publish_no_build
}



# Set the port variable default to 22 if not set
if [ -z $port ];
then port=22
fi

help="
Invalid usage

Publish TCEP script

Usage: ./publish_tcep.sh <COMMAND>

Available COMMAND options:
setup: Installs docker resources on every node
publish: Publish the application on the cluster and run it
take_down: Delete docker swarm cluster
all: Run all steps to publish the cluster and start the application
"

if [ -z ${CMD} ]; then
    echo "$help"
    exit 1
fi

if [ ${CMD} == "publish" ]; then publish
elif [ ${CMD} == "setup" ]; then setup
elif [ ${CMD} == "all" ]; then all
elif [ ${CMD} == "take_down" ]; then take_down_swarm
elif [ ${CMD} == "init_manager" ]; then init_manager
elif [ ${CMD} == "reboot" ]; then rebootSwarm
elif [ ${CMD} == "build_remote" ]; then build_remote
elif [ ${CMD} == "get_output" ]; then get_output
else ${CMD}
fi
