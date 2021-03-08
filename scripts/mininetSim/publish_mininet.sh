#!/usr/bin/env bash

# Author: Niels Danger
# Description: provides functionality for copying, setting up and running mininet/containernet simulation of TCEP
#              as well as running learning data collection simulations in parallel on multiple remote machines
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../.."
source "${work_dir}/scripts/templates/docker-swarm-maki.cfg"

u=$2 # username on remote VMs
#all_vms=("10.2.1.42" "10.2.1.40" "10.2.1.15")
all_vms=("10.2.1.40" "10.2.1.15")

if [ ! -z "$3" ]
  then
    machine=$3
  else machine="${all_vms[0]}"
fi
if [ ! -z "$4" ]; then
    nSpeedPublishers=$4
else
    nSpeedPublishers=8
fi
if [ ! -z "$5" ]; then
  nPublishersPerSection=$5
else
  nPublishersPerSection=2
fi
if [[ $((nSpeedPublishers % nPublishersPerSection)) -ne 0 ]]; then
  echo "number of publishers must be evenly divisible by number of publishers per section!"
  exit
fi
if [ ! -z "$6" ]
  then
    duration=$6
  else duration=25
fi
if [ ! -z "$7" ]
  then
    algorithm=$7
  else algorithm="MDCEP"
fi
if [ ! -z "$8" ]
  then
    latency=$8
  else latency=10
fi
if [ ! -z "$9" ]
  then
    script=$9
  else script="run_mininet_simulation_madrid.sh"
fi

mobility="True"
rsus=$((nSpeedPublishers / nPublishersPerSection))
innerNodesL1=$rsus
innerNodesL2=$(($(($innerNodesL1 > 3)) * $((innerNodesL1 / 2)))) # second layer is only added if more than 3 nodes in layer1
# cars + density publishers +  rsus + inner nodes; nCars = nRSUs
nNodesTotal=$((nSpeedPublishers + 1 + innerNodesL1 + innerNodesL2 + 1))
build_remote='false' # set true if developing under windows; builds docker image on remote host
HOST=${u}@${machine}

all() {
  if [[ $machine == "localhost" ]]; then
    echo "running simulation on localhost -> removing running docker containers before build"
    ssh -t $HOST 'docker rm -f $(docker ps -a -q)'
  fi
  compile && copy $machine && publish
}
# runs a single simulation with the given settings
publish() {
    echo "setting up ${duration}m $algorithm simulation with mobility=$mobility on the KOM VM"
    ssh -t $HOST 'bash -s' < ./${script} ${duration} ${algorithm} ${latency} ${mobility} ${nSpeedPublishers} ${nPublishersPerSection}
}

# starts a series of remote simulation runs
# !! does not work reliably, better to start 'nohup python run_datacollection.py ${mobile} &' directly on the remote machines !!
run_datacollection_remote() {
    local host=$1
    local mobile=$2
    echo "running data collection simulations on the KOM VM $host, individual simulations run by script on VM"
    echo "this will take a long time..."
    copy ${host}
    nohup ssh ${u}@${host} "nohup python run_datacollection.py ${mobile} ${script} ${duration} &" &
}

# runs data collection simulations with the specified mobility simulation settings for all algorithms given in run_datacollection.py
run_all_datacollections() {

    compile
    copyToAllMachines
    echo "running data collections in parallel on all available vms"
    run_datacollection_remote "10.2.1.15" "True"
    run_datacollection_remote "10.2.1.40" "True"
    run_datacollection_remote "10.2.1.42" "True"
    run_datacollection_remote "10.2.1.44" "False"
    run_datacollection_remote "10.2.1.48" "False"
    run_datacollection_remote "10.2.1.64" "False"
}

# runs data collection simulations with the specified algorithm, mobility and topology settings
run_specific_datacollection_remote() {

    local host=$1
    local algorithm=$2
    local mobile=$3
    compile && copy ${host}
    echo "running data collection simulations on the KOM VM $host, individual simulations run by script on VM"
    echo "this will take a long time..."

    nohup ssh ${u}@${host} "nohup python run_specific_datacollection.py ${mobile} ${script} ${algorithm} 0 &" &
}

# used for filling data gaps from too many runs of a specific setup failing
run_specific_datacollections() {
    echo "running data collections in parallel on all available vms"
    run_specific_datacollection_remote "10.2.1.15" "Relaxation" "True"
    run_specific_datacollection_remote "10.2.1.40" "Relaxation" "True"
    run_specific_datacollection_remote "10.2.1.42" "Relaxation" "True"
    run_specific_datacollection_remote "10.2.1.44" "Relaxation" "False"
    run_specific_datacollection_remote "10.2.1.48" "Relaxation" "False"
    run_specific_datacollection_remote "10.2.1.64" "Relaxation" "False"
}

adjust_config() {
    local sections=$rsus
    echo "replacing gui server in config with $machine"
    echo "configuring application.conf for $nSpeedPublishers speed publishers and $nNodesTotal nodes total"
    sed -i -r "s#mininet-simulation = .*#mininet-simulation = true#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#min-nr-of-members = [0-9]*#min-nr-of-members = $nNodesTotal#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#number-of-speed-publisher-nodes = [0-9]*#number-of-speed-publisher-nodes = $nSpeedPublishers#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#number-of-road-sections = [0-9]*#number-of-road-sections = $sections#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#gui-endpoint = \"(.*?)\"#gui-endpoint = \"http://10.0.0.254:3000\"#" ${work_dir}/src/main/resources/application.conf
    sed -i -r 's| \"akka\.tcp://tcep@simulator:\"\$\{\?constants\.base-port\}\"\"| #\"akka.tcp://tcep@simulator:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
    sed -i -r 's| #\"akka\.tcp://tcep@10\.0\.0\.253:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@10.0.0.253:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#const SERVER = \"(.*?)\"#const SERVER = \"${machine}\"#" ${work_dir}/gui/constants.js
    sed -i -r "s#const SERVER = \"(.*?)\"#const SERVER = \"${machine}\"#" ${work_dir}/gui/src/graph.js
}

compile() {
    adjust_config && echo "successfully adjusted application.conf" && \
    ${work_dir}/scripts/build.sh $build_remote && echo "successfully built fatjar" && \
    ./build_containernet_dockerimage.sh
}

# copy all necessary files to remote machine and build custom containernet image
copy () {
    local host=${u}@$1
    # install pip and docker if not present
    echo "checking docker and pip version..."
    ssh $host "pip3 --version" || ssh $host -tt "sudo apt-get update && sudo apt install -y python3-pip && pip3 install pandas APScheduler"
    ssh $host "docker --version" || ssh $host -tt <<-'ENDSSH'
    if ! [ -x "$(command -v docker)" ]; then
        # Update the apt package index
        sudo apt-get update

        # Install packages to allow apt to use a repository over HTTPS
        sudo apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common

        # Add Docker’s official GPG key
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

        # Use the following command to set up the stable repository
        sudo add-apt-repository \
        "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) \
        stable"

        # Update the apt package index
        sudo apt-get update

        # Install the latest version of Docker CE
        sudo apt-get install docker-ce -y

        # Create the docker group.
        sudo groupadd docker

        # Add your user to the docker group.
        sudo usermod -aG docker $USER

        # install missing kernelmodules and openvswitch since they can't be included with the container
        wget -O ~/tcep/install.sh https://raw.githubusercontent.com/containernet/containernet/master/util/install.sh
        chmod u+x ~/tcep/install.sh
        sudo ~/tcep/install.sh -v
    else
        echo "Docker already installed on $1"
        sudo usermod -a -G docker $USER
    fi
ENDSSH
    echo "clearing previous runs log files..."
    ssh ${host} rm -rfd ~/logs_backup/
    ssh ${host} mkdir ~/logs_backup/
    echo "copying files to ${host}..."
    ssh $host mkdir -p ~/scripts/mininetSim/mycontainernet
    scp -r $work_dir/scripts/mininetSim/mycontainernet $host:~/scripts/mininetSim/

    # this should only be executed on initial copy
    ssh $host [ ! -d ~/tcep ] \
    && ssh $host mkdir ~/tcep \
    && ssh $host mkdir ~/tcep/mobility_traces

    if [[ $wifi == "True" ]]; then
        ssh $host [ ! -d ~/tcep/mininet-wifi ] \
        && ssh $host 'cd ~/tcep && git clone https://github.com/intrig-unicamp/mininet-wifi.git && cd ~' \
        && ssh -t ${host} 'sudo ~/tcep/mininet-wifi/util/install.sh -Wlnfv' \
        && scp -r mininet-wifi ${host}:~/tcep/ \
        && echo "successfully copied mininet-wifi on VM" \
        && ssh -t ${host} 'sudo ~/tcep/mininet-wifi/util/install.sh -n' \
        && echo "successfully installed mininet-wifi on VM"
    fi
    ssh $host 'touch ~/tcep/handovers.csv'
    scp mycontainernet/containernet_entrypoint.sh ${work_dir}/src/main/resources/application.conf mycontainernet/mobility.py ${host}:~/tcep/
    scp *.py *.sh ${host}:~/tcep/
    scp $work_dir/docker-swarm.cfg ${host}:~/tcep/
    tracefile='A6-d11-h08.dat'
    if [[ ! -f $tracefile ]]; then
        wget http://www.it.uc3m.es/madrid-traces/traces/short_traces.tar.gz.part00 \
        && wget http://www.it.uc3m.es/madrid-traces/traces/short_traces.tar.gz.part01 \
        && wget http://www.it.uc3m.es/madrid-traces/traces/short_traces.tar.gz.part02 \
        && wget http://www.it.uc3m.es/madrid-traces/traces/short_traces.tar.gz.part03 \
        && wget http://www.it.uc3m.es/madrid-traces/traces/short_traces.tar.gz.part04 \
        && cat short_traces.tar.gz.part* > short_traces.tar.gz \
        && tar -zxvf short_traces.tar.gz -C . $tracefile \
        && rm short_traces.tar.gz.part* \
        && rm short_traces.tar.gz
    fi

    ssh $host "[ ! -f ~/tcep/mobility_traces/$tracefile ]" \
    && scp $tracefile ${host}:~/tcep/mobility_traces/
    scp *.csv ${host}:~/tcep/mobility_traces

    ssh ${host} "chmod +x ~/tcep/*.py"
    ssh ${host} "chmod +x ~/tcep/*.sh"
    echo "scripts made executable"

}

copyToAllMachines() {

    for host in "${all_vms[@]}"
    do
        copy ${host} &
    done
}

install_mininet-wifi_on_vm() {
    ssh -t $HOST "sudo rm -rfd ~/tcep ~/mac80211_hwsim_mgmt ~/openflow ~/wmediumd"
    copy ${machine} && echo "successfully installed mininet-wifi with docker container support"
}

stop () {
	echo "stopping containers"
	ssh  $HOST docker stop $(docker ps --filter ancestor=tcep -a -q)
}

# fetch results from data collection simulations on remote machines
fetch_datacollection() {

    local datetime=$(date +%Y%m%d-%H:%M:%S)
    mkdir -p $work_dir/splc/data_collection/${datetime}
    for i in "${all_vms[@]}"
    do
        local host="${u}@${i}"
        echo "fetching results from vm $host"
        ssh ${host} "cp -rf ~/logs/ ~/logs_backup/$datetime" # move last run
        ssh ${host} "mkdir -p ~/data_collection/$datetime"
        ssh ${host} "find ~/logs_backup -name '*data_collection*' -type f -size +5000c -exec cp {} /home/$u/data_collection/$datetime \;"
        # copy cfm.xml
        ssh ${host} "find ~/logs_backup -name 'cfm.xml' -type f -exec cp {} /home/$u/data_collection/$datetime \;"
        scp ${host}:~/data_collection/$datetime/cfm.xml $work_dir/splc/cfm.xml
        scp -r ${host}:~/data_collection/$datetime $work_dir/splc/data_collection/
        echo "\n clearing data from runs in logs_backup and logs"
        ssh ${host} "rm -rfd ~/logs_backup && mkdir ~/logs_backup" # clear data from runs
        ssh ${host} "rm ~/logs"
        ssh ${host} "mkdir ~/logs"
    done
}

# created by Benedikt Lins
fetch_datacollection_custom() {

    local datetime=$(date +%Y%m%d-%H:%M:%S)
    mkdir -p $work_dir/data_collection/${datetime}
    for i in "${all_vms[@]}"
    do
        local host="${u}@${i}"
        echo "fetching results from vm $host"
        ssh ${host} "cp -rf ~/logs/ ~/logs_backup/$datetime" # move last run
        ssh ${host} "mkdir -p ~/data_collection/$datetime"
        ssh ${host} "find ~/logs_backup -name '*data_collection*' -type f -size +5000c -exec cp {} /home/$u/data_collection/$datetime \;"
        # copy cfm.xml
        ssh ${host} "find ~/logs_backup -name 'cfm.xml' -type f -exec cp {} /home/$u/data_collection/$datetime \;"
        scp ${host}:~/data_collection/$datetime/cfm.xml $work_dir/data_collection/cfm.xml
        scp -r ${host}:~/data_collection/$datetime $work_dir/data_collection/
        echo "\n clearing data from runs in logs_backup and logs"
        #ssh ${host} "rm -rfd ~/logs_backup && mkdir ~/logs_backup" # clear data from runs
        ssh ${host} "rm ~/logs"
        ssh ${host} "mkdir ~/logs"
    done
}

# created by Benedikt Lins
fetch_datacollection_custom() {

    local datetime=$(date +%Y%m%d-%H:%M:%S)
    mkdir -p $work_dir/data_collection/${datetime}
    for i in "${all_vms[@]}"
    do
        local host="${u}@${i}"
        echo "fetching results from vm $host"
        ssh ${host} "cp -rf ~/logs/ ~/logs_backup/$datetime" # move last run
        ssh ${host} "mkdir -p ~/data_collection/$datetime"
        ssh ${host} "find ~/logs_backup -name '*data_collection*' -type f -size +5000c -exec cp {} /home/$u/data_collection/$datetime \;"
        # copy cfm.xml
        ssh ${host} "find ~/logs_backup -name 'cfm.xml' -type f -exec cp {} /home/$u/data_collection/$datetime \;"
        scp ${host}:~/data_collection/$datetime/cfm.xml $work_dir/data_collection/cfm.xml
        scp -r ${host}:~/data_collection/$datetime $work_dir/data_collection/
        echo "\n clearing data from runs in logs_backup and logs"
        #ssh ${host} "rm -rfd ~/logs_backup && mkdir ~/logs_backup" # clear data from runs
        ssh ${host} "rm ~/logs"
        ssh ${host} "mkdir ~/logs"
    done
}

#combine_datacollection() {
#
#   filter_datacollection $1
#   # combine all files
#   find "${datacollection_folder}_filtered/mobility" -type f -name *_splc.csv* \
#   -exec cat {} + >> "${datacollection_folder}_filtered/combined_mobility_measurements.csv"
#   find "${datacollection_folder}_filtered/no_mobility" -type f -name *_splc.csv* \
#   -exec cat {} + >> "${datacollection_folder}_filtered/combined_no_mobility_measurements.csv"
#   # remove redundant header lines
#   # note: this is for data that has no 'fcMobility' in the header yet
#	header='root;fsSystem;fsMechanisms;fsPlacementAlgorithm;fsMDCEP;fsRelaxation;fsRandom;fsProducerConsumer;fcContext;fcNetworkSituation;fcFixedProperties;fcVariableProperties;fcOperatorTreeDepth;fcMaxTopoHopsPubToClient;fcBaseLatency;fcJitter;fcLoadVariance;fcEventPublishingRate;fcAvgEventArrivalRate;fcNodeCount;fcNodeCountChangerate;fcLinkChanges;fcNodeToOperatorRatio;fcAvgVivaldiDistance;fcVivaldiDistanceStdDev;fcMaxPublisherPing;fcGiniNodesIn1Hop;fcGiniNodesIn2Hop;fcAvgNodesIn1Hop;fcAvgNodesIn2Hop;mLatency;mAvgSystemLoad;mMsgHops;mOverhead;mNetworkUsage'
#   sed --in-place "/"$header"/d" "${datacollection_folder}_filtered/combined_mobility_measurements.csv"
#   sed --in-place "/"$header"/d" "${datacollection_folder}_filtered/combined_no_mobility_measurements.csv"
#   #re-add header once
#   newHeader='root;fsSystem;fsMechanisms;fsPlacementAlgorithm;fsMDCEP;fsRelaxation;fsRandom;fsProducerConsumer;fcContext;fcNetworkSituation;fcFixedProperties;fcVariableProperties;fcMobility;fcOperatorTreeDepth;fcMaxTopoHopsPubToClient;fcBaseLatency;fcMobility;fcJitter;fcLoadVariance;fcEventPublishingRate;fcAvgEventArrivalRate;fcNodeCount;fcNodeCountChangerate;fcLinkChanges;fcNodeToOperatorRatio;fcAvgVivaldiDistance;fcVivaldiDistanceStdDev;fcMaxPublisherPing;fcGiniNodesIn1Hop;fcGiniNodesIn2Hop;fcAvgNodesIn1Hop;fcAvgNodesIn2Hop;mLatency;mAvgSystemLoad;mMsgHops;mOverhead;mNetworkUsage'
#   echo $header | cat - "${datacollection_folder}_filtered/combined_mobility_measurements.csv" > temp \
#   && mv temp "${datacollection_folder}_filtered/combined_mobility_measurements.csv"
#   cat "${datacollection_folder}_filtered/combined_mobility_measurements.csv" "${datacollection_folder}_filtered/combined_no_mobility_measurements.csv" > "${datacollection_folder}_filtered/combined_measurements.csv"
#
#
help="call with the following arguments in this order:
        command {all, compile, copy, publish, all_changing, all_datacollection, publish_changing, publish_datacollection, stop},
        username,
        VM IP, {10.2.1.4X},
        simulationDuration {1 to x minutes},
        initialPlacementAlgorithm {Starks, Pietzuch} - irrelevant if calling all or publish"


if [ $1 == "publish" ]; then publish
elif [ $1 == "compile" ]; then compile
elif [ $1 == "copy" ]; then copy $machine
elif [ $1 == "copyToAllMachines" ]; then copyToAllMachines
elif [ $1 == "all" ]; then all
elif [ $1 == "stop" ]; then stop
elif [ $1 == "run_datacollection" ]; then run_datacollection_remote ${machine} ${topology} # starting remote datacollections sometimes does not work properly, better run command directly on remote with nohup ... &
elif [ $1 == "run_all_datacollections" ]; then run_all_datacollections # unreliable, better run 'nohup python run_datacollection ${mobility} 'Tree' &' on each remote machine
elif [ $1 == "run_specific_datacollections" ]; then run_specific_datacollections
elif [ $1 == "fetch_datacollection" ]; then fetch_datacollection_custom
elif [ $1 == "filter_datacollection" ]; then filter_datacollection $2
elif [ $1 == "config" ]; then adjust_config
elif [ $1 == "install_mininet-wifi" ]; then install_mininet-wifi_on_vm
else echo ${help}
fi
