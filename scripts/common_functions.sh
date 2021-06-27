if [ -z $port ];
then port=22
fi

setup_docker_remote() {
  ssh $1 "docker --version" || ssh $1 -tt <<-'ENDSSH'
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
}

setup_sumo() {
  ssh $1 "sumo-gui --version" || ssh $1 -tt 'sudo LC_ALL=C.UTF-8 add-apt-repository -y ppa:sumo/stable && \
    sudo apt-get update && \
    sudo apt-get install -y sumo sumo-tools'
}

adjust_config() {
    local sections=$1
    local nSpeedPublishers=$2
    local nNodesTotal=$3
    local gui_host=$4
    local mininet=$5
    local wifi=$6
    local gui_port=$7
    local prediction_port=$8
    if [ -z $7 ]; then
      gui_port=3000
    fi
    if [ -z $8 ]; then
      prediction_port=9091
    fi
    echo "configuring application.conf for $nSpeedPublishers speed publishers and $nNodesTotal containers total"
    sed -i -r "s#mininet-simulation = .*#mininet-simulation = ${mininet}#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#isLocalSwarm = .*#isLocalSwarm = false#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#min-nr-of-members = [0-9]*#min-nr-of-members = $nNodesTotal#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#number-of-speed-publisher-nodes = [0-9]*#number-of-speed-publisher-nodes = $nSpeedPublishers#" ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#number-of-road-sections = [0-9]*#number-of-road-sections = $sections#" ${work_dir}/src/main/resources/application.conf
    sed -i -r 's| \"akka\.tcp://tcep@| #\"akka.tcp://tcep@|' ${work_dir}/src/main/resources/application.conf
    sed -i -r "s#const SERVER = \"(.*?)\"#const SERVER = \"${gui_host}\"#" ${work_dir}/gui/src/graph.js
    sed -i -r "s#const GUI_PORT = .*#const GUI_PORT = ${gui_port}#" ${work_dir}/gui/src/graph.js
    sed -i -r "s#const GUI_PORT = .*#const GUI_PORT = ${gui_port}#" ${work_dir}/gui/constants.js
    # use perl since sed is a pia to use for multi-line matching
    perl -0777 -i -pe "s/tcep-gui\n\s+ports:\s+-\s[0-9]+:[0-9]+/tcep-gui\n    ports:\n      - ${gui_port}:${gui_port}/igs" ${work_dir}/docker-stack.yml

    if [[ $mininet == "true" ]]; then # mininet simulation
      sed -i -r "s#gui-endpoint = \"(.*?)\"#gui-endpoint = \"http://${gui_host}:${gui_port}\"#" ${work_dir}/src/main/resources/application.conf
   	  sed -i -r "s#prediction-endpoint = \"(.*?)\"#prediction-endpoint = \"http://${gui_host}:${prediction_port}\"#" ${work_dir}/src/main/resources/application.conf

      if [[ $wifi == "true" ]]; then
        sed -i -r 's| #\"akka\.tcp://tcep@20\.0\.0\.15:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@20.0.0.15:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
        sed -i -r "s#const TCEP_SERVER = \"(.*?)\"#const TCEP_SERVER = \"20.0.0.15\"#" ${work_dir}/gui/constants.js
        sed -i -r "s#const INTERACTIVE_SIMULATION_ENABLED = (.*?)#const INTERACTIVE_SIMULATION_ENABLED = false#" ${work_dir}/gui/src/graph.js
        sed -i -r "s#const INTERACTIVE_SIMULATION_ENABLED = (.*?)#const INTERACTIVE_SIMULATION_ENABLED = false#" ${work_dir}/gui/constants.js
      else
        sed -i -r 's| #\"akka\.tcp://tcep@10\.0\.0\.253:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@10.0.0.253:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
        sed -i -r "s#const TCEP_SERVER = \"(.*?)\"#const TCEP_SERVER = \"10.0.0.253\"#" ${work_dir}/gui/constants.js
        sed -i -r "s#const INTERACTIVE_SIMULATION_ENABLED = (.*?)#const INTERACTIVE_SIMULATION_ENABLED = true#" ${work_dir}/gui/src/graph.js
        sed -i -r "s#const INTERACTIVE_SIMULATION_ENABLED = (.*?)#const INTERACTIVE_SIMULATION_ENABLED = true#" ${work_dir}/gui/constants.js
      fi
    else # docker-swarm simulation
      sed -i -r 's| #\"akka\.tcp://tcep@simulator:\"\$\{\?constants\.base-port\}\"\"| \"akka.tcp://tcep@simulator:\"${?constants.base-port}\"\"|' ${work_dir}/src/main/resources/application.conf
      sed -i -r "s#const TCEP_SERVER = \"(.*?)\"#const TCEP_SERVER = \"simulator\"#" ${work_dir}/gui/constants.js
      sed -i -r "s#const INTERACTIVE_SIMULATION_ENABLED = (.*?)#const INTERACTIVE_SIMULATION_ENABLED = true#" ${work_dir}/gui/src/graph.js
      sed -i -r "s#const INTERACTIVE_SIMULATION_ENABLED = (.*?)#const INTERACTIVE_SIMULATION_ENABLED = true#" ${work_dir}/gui/constants.js
   	  sed -i -r "s#gui-endpoint = \"(.*?)\"#gui-endpoint = \"http://gui:${gui_port}\"#" ${work_dir}/src/main/resources/application.conf
   	  sed -i -r "s#prediction-endpoint = \"(.*?)\"#prediction-endpoint = \"http://predictionEndpoint:${prediction_port}\"#" ${work_dir}/src/main/resources/application.conf
   	  if [[ $manager == "localhost" ]]; then
   	    sed -i -r "s#isLocalSwarm = .*#isLocalSwarm = true#" ${work_dir}/src/main/resources/application.conf
   	  fi
    fi

}


setup_docker() {
    echo "Setting up docker on instance $1"
    ssh-keyscan -H $1 >> ~/.ssh/known_hosts
    ssh ${2}@${1} "mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys" && cat ~/.ssh/id_rsa.pub | ssh ${2}@${1} 'cat >> ~/.ssh/authorized_keys'
    ssh $2@$1 "mkdir -p ~/logs"
    #configure_passwordless_sudo $1
    ssh -t -p $port $2@$1 "sudo hostname node$3"
    #ssh -T -p $port $2@$1 "grep -q -F '127.0.0.1 $3' /etc/hosts || sudo bash -c \"echo '127.0.0.1 $3' >> /etc/hosts\""
    ssh -T -p $port $2@$1 <<-'ENDSSH'
        mkdir -p ~/src && mkdir -p ~/logs

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

        #Install docker-compose version 1.17
        sudo curl -L https://github.com/docker/compose/releases/download/1.17.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

    else
        echo "Docker already installed on $1"
        sudo usermod -a -G docker $USER
    fi
ENDSSH
}

configure_passwordless_sudo() {
    local host=$1
    #line="$user ALL=(ALL:ALL) NOPASSWD: /usr/bin/python2.7, /usr/bin/mn, /usr/local/bin/mn /bin/cat /usr/sbin/visudo"
    line="$user ALL=(ALL:ALL) NOPASSWD: ALL"
    if [[ -z $(ssh ${user}@${host} "sudo cat /etc/sudoers | grep $line ") ]]; then
        echo "add the following line to the end of /etc/sudoers of $host via visudo: "
        echo ${line}
        ssh -t ${user}@${host} "sudo visudo"
    fi
}
