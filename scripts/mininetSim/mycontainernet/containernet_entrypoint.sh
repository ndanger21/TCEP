#! /bin/bash -e

# start OVS
service openvswitch-switch start && echo "successfully started openvswitch-switch service"

# check if docker socket is mounted
if [ ! -S /var/run/docker.sock ]; then
    echo 'Error: the Docker socket file "/var/run/docker.sock" was not found. It should be mounted as a volume.'
    exit 1
fi

# start floodlight controller in background
#cd /floodlight && nohup java -jar /floodlight/target/floodlight.jar &
#echo "Floodlight controller running in background..."

echo "Welcome to Containernet running within a Docker container ..."

truncate -s 0 /containernet/tcep/nohup.out

if [[ $# -eq 0 ]]; then
    exec /bin/bash
else
    exec $*
fi
