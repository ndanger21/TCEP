#!/bin/bash
source "docker-swarm_geni_yahoo.cfg"
nPublisherOps=8
nPublisherHosts=4
CMD=$1
if [ -z $CMD ]; then
  CMD="publish"
fi
work_dir="$(pwd -P)/../.."

for (( i=1; i<=${nPublisherOps}; i++ ))
do
  if [ ! -f "yahooTraceP$i.csv" ]; then
    echo "trace file yahooTraceP$i does not exist, creating all trace files..."
    python YahooStreaming.py
  fi
done

if [ ! -f traces.zip ]; then
  zip -r -o traces.zip *
fi
#
#p_count=1
#for i in "${workers[@]}"
#do
#  ssh ${user}@${i} "mkdir -p ~/tcep/event_traces"
#  ssh ${user}@${i} [ -f '~/tcep/event_traces/traces.zip' ] || \
#  scp traces.zip $user@$i:'~/tcep/event_traces/traces.zip' && \
#  echo "transferred traces to $i" && \
#  ssh ${user}@${i} [ -f '~/tcep/event_traces/yahooJoins.csv' ] || \
#  ssh ${user}@${i} "unzip -o ~/tcep/event_traces/traces.zip -d ~/tcep/event_traces/" && \
#  echo "unzipped traces"
#  #if [ $p_count -le $nPublisherHosts ]; then
#  #  ssh ${user}@${i} "mkdir -p ~/tcep/event_traces"
#  #  ssh ${user}@${i} [ -f '~/tcep/event_traces/yahooTraceP'$p_count'.csv' ] || \
#  #  scp "yahooTraceP$p_count.csv" ${user}@${i}:~/tcep/event_traces/ && \
#  #  echo "copied yahooTraceP$p_count to $i"
#  #  # 8 publishers, only 4 publisher hosts -> ops 5,6,7,8 on hosts 1,2,3,4
#  #  # 8 publishers, only 5 publisher hosts -> ops 6,7,8 on hosts 1,2,3
#  #  target_idx=$((p_count+nPublisherHosts))
#  #  target_file="yahooTraceP$target_idx.csv"
#  #  if [[ $nPublisherOps -gt $nPublisherHosts ]]; then
#  #    if [[ $target_idx -le $nPublisherOps ]]; then
#  #      echo "more stream ops than publisher hosts, copying $target_file to $i"
#  #      ssh ${user}@${i} [ -f '~/tcep/event_traces/yahooTraceP'$target_idx'.csv' ] || \
#  #      scp "$target_file" ${user}@${i}:~/tcep/event_traces/
#  #    fi
#  #  fi
#  #fi
#  p_count=$((p_count + 1))
#done
#ssh ${user}@${manager} "mkdir -p ~/tcep/event_traces"
#scp yahooJoins.csv ${user}@${manager}:~/tcep/event_traces/

bash ${work_dir}/scripts/publish_tcep.sh ${CMD} --stack-file "scenarios/yahoo_streaming/docker-stack_geni_yahoo.yml" --cfg "/scenarios/yahoo_streaming/docker-swarm_geni_yahoo.cfg" -h $manager -u $user
#bash ${work_dir}/scripts/publish_tcep.sh ${CMD} --stack-file "scenarios/yahoo_streaming/docker-stack_geni_yahoo.yml" --cfg "/scenarios/yahoo_streaming/docker-swarm_maki_yahoo.cfg" -h $manager -u $user