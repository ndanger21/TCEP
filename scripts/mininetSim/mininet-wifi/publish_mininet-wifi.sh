#!/bin/bash
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../../.."
source "${work_dir}/docker-swarm.cfg"
source ${work_dir}/scripts/common_functions.sh

echo "publish_mininet-wifi.sh args: $@"
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
    -c|--controller)
    controller_ip="$2"
    shift # past argument
    shift # past value
    echo "parsed controller $controller_ip, \n args: $@"
    ;;
    -h|--host)
    machine="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--duration)
    duration="$2"
    shift # past argument
    shift # past value
    ;;
    -m|--mapek)
    mapek="$2"
    shift
    shift
    ;;
    -r|--req)
    req="$2"
    shift
    shift
    ;;
    -q|--query)
    query="$2"
    shift
    shift
    ;;
    -a|--algo)
    algorithm="$2"
    shift
    shift
    ;;
    -s|--sumo-gui)
    sumo_gui="True"
    shift
    ;;
    -p|--publishers)
    nSpeedPublishers="$2"
    shift
    shift
    ;;
    -w|--workers)
    nRSUs="$2"
    shift
    shift
    ;;
    -t|--transitionStrategy)
    transitionStrategy="$2"
    shift
    shift
    ;;
    -tex|--transitionExecutionMode)
    transitionExecutionMode="$2"
    shift
    shift
    ;;
    -e|--eventrate)
    eventrate="$2"
    shift
    shift
    ;;
    --default)
    DEFAULT=YES
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done

echo "parsed args: $u $machine $mapek $req $query $controller_ip $algorithm"

if [ -z "$u" ]; then
  u=$USER
fi
if [ -z "$machine" ]; then
   machine="localhost"
fi
if [ -z "$duration" ]; then
   duration=2
fi
if [ -z "$mapek" ]; then
    mapek='requirementBased'
fi
if [ -z "$req" ]; then
    req="hops"
fi
if [ -z "$query" ]; then
  query="Conjunction"
fi
if [ -z "$algorithm" ]; then
   algorithm="Relaxation"
fi
if [ -z "$controller_ip" ]; then
  controller_ip="10.0.30.15"
fi
if [ -z "$nSpeedPublishers" ]; then
  nSpeedPublishers=8
fi
if [ -z "$nRSUs" ]; then
  nRSUs=4  # worker nodes on the 12 APs
fi
if [ -z "$sumo_gui" ]; then
  sumo_gui="False"
fi
if [ -z "$transitionStrategy" ]; then
  transitionStrategy="MFGS"
fi
if [ -z "$transitionExecutionMode" ]; then
  transitionExecutionMode="1"
fi
if [ -z "$eventrate" ]; then
  eventrate="10"
fi
host=${u}@${machine}
if [[ ${controller_ip} == "localhost" ]] || [[ ${controller_ip} == "127.0.0.1" ]]; then
  gui_ip="172.30.0.254"
else
  gui_ip=${controller_ip}
fi
echo "all args: $host $duration $mapek $req $query $algorithm $controller_ip $nSpeedPublishers $nRSUs $sumo_gui"


compile() {
  echo "adjusting application.conf"
	# docker container for gui has different ip if controller runs locally

  adjust_config 12 ${nSpeedPublishers} $(($nRSUs + $nSpeedPublishers + 3)) ${gui_ip} "true" "true" # there are always 12 aps (not all have a rsu); gui is run same host as controller
  echo "Building jarfile..."
  pushd ${work_dir} && sbt assembly || exit 1
  popd
  sha1sum -c prevBuildHash.sha
  isBuildUnchanged=$?
  sha1sum ${work_dir}/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar > prevBuildHash.sha
  echo "building gui docker image..."
  pushd $work_dir/gui && \
  docker build -t $registry_user/$gui_image . && \
  docker login && docker push $registry_user/$gui_image
  popd
}

setup() {
  ssh $host [ ! -d ~/tcep ] && ssh $host 'mkdir ~/tcep'
  echo "checking python pip version..."
  ssh $host "pip --version" || ssh $host -tt "sudo apt-get update && sudo apt install -y python-pip && pip install APScheduler"
  setup_docker_remote $host
  setup_sumo $host

  ssh $host [ ! -d ~/tcep/mininet-wifi ] \
  && ssh $host 'cd ~/tcep && git clone https://github.com/intrig-unicamp/mininet-wifi.git && cd ~/tcep/mininet-wifi && git checkout e46a4eb74027842c1dd6d94a75775a7d011d3bde' \
  && ssh -t ${host} 'sudo ~/tcep/mininet-wifi/util/install.sh -Wln3fv && sudo apt-get update && sudo apt-get install openntpd' \
  && echo "successfully installed mininet-wifi on ${host}" # && \
  pwd
  scp -rd examples/ mn_wifi/ $host:~/tcep/mininet-wifi/ \
  && scp -r patches/ $host:~/tcep/mininet-wifi/ \
  && ssh -t ${host} 'cd ~/tcep/mininet-wifi/patches/ \
                    && sudo patch -N ../mn_wifi/telemetry.py plot_no_vehicle_range.patch \
                    && sudo patch -N ../mininet/mininet/nodelib.py no_nat_networkManager_restart.patch \
                    && cd ~/tcep/mininet-wifi/mininet && sudo make install \
                    && cd ~/tcep/mininet-wifi && sudo make install' \
  && echo "successfully patched mininet wifi with custom changes for sumo sim" \
  && scp -r ${work_dir}/mobilityTraces/ $host:~/tcep/mininet-wifi/mobility_traces/ \
  && scp  *.py $host:~/tcep/mininet-wifi/ && \
  scp mn_wifi/*.py $host:~/tcep/mininet-wifi/mn_wifi/ && \
  scp mn_wifi/sumo/*.py $host:~/tcep/mininet-wifi/mn_wifi/sumo/ && \
  ssh -t ${host} 'cd ~/tcep/mininet-wifi && sudo make install'

  scp  *.py $host:~/tcep/mininet-wifi/
  scp ${work_dir}/src/main/resources/application.conf $host:~/tcep/

  ssh $host 'touch ~/tcep/handovers.csv && chmod a+w ~/tcep/handovers.csv && truncate -s 0 ~/tcep/handovers.csv'
  # only re-transmit jarfile if build has changed
  if [ ${isBuildUnchanged} == 0 ]; then
    echo "build unchanged, not copying jarfile to ${host}"
  else
    echo "build has changed, copying jar to ${host}..." && \
    scp ${work_dir}/src/main/resources/application.conf ${work_dir}/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar \
      ${host}:~/tcep/
  fi

  #ssh $host '[ -z $(docker ps -a -q --filter ancestor=cloudwattfr/ntpserver) ] &&
  #          echo "starting ntp container" &&
  #          docker run --name nserver -d -p 123:123  cloudwattfr/ntpserver:latest'

}

start_controller() {
  controller_host=$u@${controller_ip}
  ssh ${controller_host} '[ -z $(docker ps -a -q --filter ancestor=onosproject/onos) ] &&
            echo "starting ONOS SDN controller on '$u@${controller_ip}'...." &&
            docker run --rm -t -d -p 8181:8181 -p 8101:8101 -p 5005:5005 -p 830:830 -p 6653:6653 --name onos onosproject/onos && sleep 45'
  # calls to ONOS REST API after waiting for startup
  ssh ${controller_host} 'docker exec onos ./bin/onos-app -u karaf -p karaf 172.17.0.2 activate org.onosproject.workflow.ofoverlay' && \
    ssh ${controller_host} 'docker exec onos ./bin/onos-app -u karaf -p karaf 172.17.0.2 activate org.onosproject.fwd' && \
    ssh ${controller_host} 'docker exec onos ./bin/onos-app -u karaf -p karaf 172.17.0.2 activate org.onosproject.mobility' && \
    echo "activated ONOS applications: OpenflowOverlay, ReactiveForwarding, HostMobility"

}

start_gui() {
   tcep_gui=$registry_user'/'$gui_image
  ssh $host '[ ! -z $(docker ps -a -q --filter ancestor='${tcep_gui}') ] && docker rm -f tcep_gui'
  ssh ${controller_host} 'mkdir -p ~/tcep/logs/gui; docker pull '${tcep_gui}
  ssh ${controller_host} '[ -z $(docker ps -a -q --filter ancestor='${tcep_gui}') ] && \
                          docker run -d --ip 172.30.0.254 -p 3000:3000 --name tcep_gui --volume /home/'${u}'/tcep/logs/gui:/usr/src/app/log '${tcep_gui} &&
                          echo "started TCEP gui on ${gui_ip}"
  if [[ ${gui_ip} == "172.30.0.254" ]] && [[ -z $(docker network ls -q -f name=gui-network) ]]; then
    echo "gui is running on localhost, creating bridge network for the container to make it reachable from inside mininet"
    docker network create --driver bridge --subnet 172.30.0.0/24 --gateway 172.30.0.1 gui-network && \
    docker network connect --ip ${gui_ip} gui-network tcep_gui
  fi
}

run() {
  echo "backing up previous simulation log files..."
  ssh $host '[ ! -d ~/tcep/logs_backup ]' && ssh ${host} 'mkdir -p ~/tcep/logs_backup/'
  ssh $host '[ "$(ls -A ~/tcep/logs/simulation)" ] && cp -rf ~/tcep/logs/ ~/tcep/logs_backup/$(date +%Y%m%d-%H:%M:%S)'
  ssh $host 'rm -rf ~/tcep/logs/'
  ssh $host '[ ! -d ~/tcep/logs ]' && ssh ${host} 'mkdir -p ~/tcep/logs/'
  for (( i=1; i<=${nSpeedPublishers}; i++ ))
  do
    ssh $host '[ ! -d ~/tcep/logs/p'$i' ] && mkdir -p ~/tcep/logs/p'$i''
  done
  for (( i=1; i<=${nRSUs}; i++ ))
  do
    ssh $host '[ ! -d ~/tcep/logs/n'$i' ] && mkdir -p ~/tcep/logs/n'$i''
  done
  ssh $host '[ ! -d ~/tcep/logs/simulation ] && mkdir -p ~/tcep/logs/simulation'
  ssh $host '[ ! -d ~/tcep/logs/dp1 ] && mkdir -p ~/tcep/logs/dp1'
  ssh $host '[ ! -d ~/tcep/logs/gui ] && mkdir -p ~/tcep/logs/gui'
  ssh $host '[ ! -d ~/tcep/logs/consumer ] && mkdir -p ~/tcep/logs/consumer'
  ssh $host 'rm -f ~/tcep/mininet-wifi/*.pid ~/tcep/mininet-wifi/*.txt ~/tcep/mininet-wifi/*.staconf'

  echo "running mininet-wifi simulation with sumo on ${host} with params duration:${duration} algorithm:${algorithm} nSpeedPublishers:${nSpeedPublishers}"
  # kill NetworkManager since it stops mininet-wifi from working correctly
  #ssh $host -t 'pgrep NetworkManager && echo "killing NetworkManager" && sudo pkill -f NetworkManager'
  #ssh $host -t 'sudo pkill -f tcep -9'
   ssh $host -t 'echo '\
  ${duration} ${algorithm} ${nSpeedPublishers} ${u} ${sumo_gui} ${controller_ip} ${nRSUs} ${registry_user}'/'${gui_image} ${mapek} ${query} ${req} ${transitionStrategy} ${transitionExecutionMode} ${eventrate} ${gui_ip}''
  ssh $host -tt 'sudo mn -c && cd ~/tcep/mininet-wifi && sudo python wifi-sumo-simulation.py '\
  ${duration} ${algorithm} ${nSpeedPublishers} ${u} ${sumo_gui} ${controller_ip} ${nRSUs} ${registry_user}'/'${gui_image} ${mapek} ${query} ${req} ${transitionStrategy} ${transitionExecutionMode} ${eventrate} ${gui_ip}''
}

all() {
  compile && \
  setup && \
  start_controller && \
  start_gui && \
  run
}

if [ -z $CMD ]; then
    all
else
    $CMD
fi
