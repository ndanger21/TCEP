#!/usr/bin/env bash
work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"
source "$work_dir/docker-swarm.cfg"

executeCommandOnAll() {
  local cmd=$@
  ssh $user@$manager "$cmd"
  for i in "${workers[@]}"
  do
    ssh $user@$i "$cmd"
  done
}

executeCommandOnAll $@