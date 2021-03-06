work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"
source "$work_dir/docker-swarm.cfg"
source "$work_dir/scripts/common_functions.sh"
# before deploying:
# check if docker-swarm.cfg and docker-stack.yml match (host = manager, docker username, number and type of nodes)
U=${user}
HOST=$manager
REMOTE=$U@${HOST}

setup_docker ${HOST} $U 0
ssh ${REMOTE} 'docker swarm init'
ssh ${REMOTE} 'docker node update --label-add subscriber=true node0'
ssh ${REMOTE} 'docker node update --label-add publisher=true node0'
ssh ${REMOTE} 'docker node update --label-add worker=true node0'
scp ../docker-stack.yml ${REMOTE}:~
nNodesTotal=$(($n_speed_streams + 2 + 1 + 1)) # speedPublishers, 2workers, 1densityPublisher, simulator
echo "number of containers: ${nNodesTotal}"
adjust_config $n_speed_streams $n_speed_streams $nNodesTotal $HOST "false" "false"
./build.sh
rm -rf $work_dir/dockerbuild
ssh ${REMOTE} "docker pull $registry_user/$tcep_image"
ssh ${REMOTE} "docker pull $registry_user/$gui_image"
#ssh ${REMOTE} "docker stack rm tcep && docker network prune"
echo "Booting up new stack"
ssh ${REMOTE} 'mkdir -p ~/logs && rm -f ~/logs/** && mkdir -p ~/src'
ssh ${REMOTE} 'docker stack deploy --with-registry-auth -c docker-stack.yml tcep'