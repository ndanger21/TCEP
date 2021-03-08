#!/usr/bin/env bash
user=$2
machine=$3
#10.0.30.10
dc_folder='../splc/data_collection'
HOST=${user}@${machine}
KEY=id_rsa

backup_and_clear() {

    ssh $HOST <<-'ENDSSH'

        if [ ! -f ~/learningsLogs_backup ]; then
            mkdir ~/learningLogs_backup
        fi
        if [ "$(ls -A ~/splc/learningLogs)" ]; then
            cp -rf ~/splc/learningLogs/ ~/learningLogs_backup/$(date +%Y-%m-%d-%H:%M:%S)
        fi
        rm -rdf ~/splc
        mkdir -p ~/splc/tmp
        mkdir ~/splc/learningLogs
        mkdir ~/splc/data_collection
        pip install pandas numpy
ENDSSH
}

# copy data and start SPLC on the remote machine
start_learning() {

    scp -r ../splc $HOST:~/
    ssh $HOST <<-'ENDSSH'
        # !!! UPDATE IF FEATURES HAVE BEEN MODIFIED !!!
        header='root;fsSystem;fsMechanisms;fsPlacementAlgorithm;fsMDCEP;fsRelaxation;fsRandom;fsProducerConsumer;fsRizou;fsGlobalOptimalBDP;fcContext;fcNetworkSituation;fcFixedProperties;fcVariableProperties;fcOperatorTreeDepth;fcMaxTopoHopsPubToClient;fcMobility;fcJitter;fcLoadVariance;fcEventPublishingRate;fcAvgEventArrivalRate;fcNodeCount;fcNodeCountChangerate;fcLinkChanges;fcNodeToOperatorRatio;fcAvgVivaldiDistance;fcVivaldiDistanceStdDev;fcMaxPublisherPing;fcGiniNodesIn1Hop;fcGiniNodesIn2Hop;fcAvgNodesIn1Hop;fcAvgNodesIn2Hop;fcAvgHopsBetweenNodes;mLatency;mAvgSystemLoad;mMsgHops;mOverhead;mNetworkUsage'

        # filter latency outliers from individual files (caused by overloaded machines)
        echo "\n filtering latency outliers from individual simulation runs, using header $header"
        cd ~/splc && find -name '*_splc.csv' -type f -size +5000c -exec python filter_outliers.py {} \;

        find ~/splc/data_collection -name '*_splc.csv' | cpio -pdm ~/splc/tmp/
        #find all files ending on .csv bigger than 5000 bytes (i.e. valid simulation runs) and concatenate them
        find ~/splc/tmp -type f -size +5000c -exec cat {} + >> ~/splc/tmp/combined_measurements.csv

        # remove redundant header lines
        sed --in-place "/"$header"/d" ~/splc/tmp/combined_measurements.csv
        #re-add header once
        echo $header | cat - ~/splc/tmp/combined_measurements.csv > temp && mv temp ~/splc/combined_measurements.csv

        echo "\n adjusting cfm.xml boundaries (to make SPLC load data without problems)"
        cd ~/splc && python adjust_boundaries.py

        docker rm -f $(docker ps --filter ancestor=martinpfannemueller/splconqueror -a -q)

        # make sure that ~/splc contains combined_measurements.csv, cfm.xml and learnScript.a
        #cd ~ && git clone https://github.com/se-passau/SPLConqueror.git
        #cd ~/SPLConqueror && docker build -t splconqueror ./
        #docker run -ti splconqueror
        docker run --name SPLC_latency --volume ~/splc:/data martinpfannemueller/splconqueror /data/learnLatency.a &
        docker run --name SPLC_load --volume ~/splc:/data martinpfannemueller/splconqueror /data/learnLoad.a &
        docker run --name SPLC_msgHops --volume ~/splc:/data martinpfannemueller/splconqueror /data/learnMsgHops.a &
        #docker run --name SPLC_msgOverhead --volume ~/splc:/data martinpfannemueller/splconqueror /data/learnMsgOverhead.a &
ENDSSH
}

# copy learned models to use into resources folder
fetch_performance_models() {
    scp -r $HOST:~/splc/learningLogs ../src/main/resources/performanceModels
}

if [ $1 == "learn" ]; then backup_and_clear && start_learning
elif [ $1 == "fetch" ]; then fetch_performance_models
else echo "unknown command!"
fi
