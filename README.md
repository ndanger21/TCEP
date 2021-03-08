# TCEP

TCEP is a research project that provides a programming model for development of operator placement algorithms for streaming applications and a means to adapt operator placement by supporting transitions. 
 
TCEP contributes the following:

+ **Programming model** to implement _operator placement_ algorithms including algorithms from the state-of-the-art (see list below)
+ **Transition execution strategies** for a _cost-efficient_ and _seamless_ operator placement transition
+ **Lightweight learning strategy** for selection of operator placement for the given QoS demands
+ **Heterogeneous infrastructure support** for execution of operator graphs

To run TCEP simply do `./scripts/build.sh && docker-compose up`. Check our <a href="http://35.246.223.49:3000/" target="_blank">live demo</a> at Google cloud! 

## List of Operator Placement Algorithms 
+ Our version of Relaxation algorithm  <a href="https://dl.acm.org/doi/10.1109/ICDE.2006.105" target="_blank">(from Pietzuch et al.)</a>
+ Our version of MDCEP algorithm  <a href="https://ieeexplore.ieee.org/document/7347944/" target="_blank">(from Starks et. al.)</a>
+ Our version of MOPA algorithm <a href="https://ieeexplore.ieee.org/document/5560127" target="_blank">(from Rizou et al.)</a>
+ Our version of Global Optimal algorithm
+ Producer Consumer operator placement
+ Random operator placement

[Video of the demo](#video-of-the-demo)

[Getting Started](#getting-started)

[Publications](#publications)

[Acknowledgement](#acknowledgement)

[Contact](#contact)

## [Video of the demo](#video-of-the-demo)
<a href="https://youtu.be/SXes2mfl-_Y" target="_blank">![TCEP-Demo](https://img.youtube.com/vi/SXes2mfl-_Y/0.jpg)</a>

## [Getting Started](#getting-started)

### Prerequisites 

* Docker v19.03 or later and a registry account
* JDK8
* sbt

### Running on a central server (centralized execution)

Simply build TCEP and run using docker-compose `./scripts/build.sh && docker-compose up`

### Running cluster simulation (distributed execution)

Adjust `$PROJECT_HOME/scripts/templates/docker-swarm.cfg` as indicated in the file 

Simulation can be executed using a GUI (mode 7) or without a GUI. The mode can be set in `$PROJECT_HOME/docker-stack.yml` file as an environment variable in simulator service.

A full list of simulations can be found below:

| Mode  | Description  |
|---|---|
| 1 | Test Relaxation algorithm  |
| 2 | Test MDCEP algorithm |
| 3 | Test SMS transition |
| 4 | Test MFGS transition |
| 5 | unavailable |
| 6 | Do nothing |
| 7 | Test with GUI |
| 8 | Test Rizou |
| 9 | Test Producer Consumer |
| 10 | Test Optimal |
| 11 | Test Random |
| 12 | Test Lightweight |

`./scripts/publish-tcep.sh all $user@$IP`

the script will deploy the docker stack defined by docker-stack.yml to the cluster formed by the VMs

### Running using GENI/ CloudLab VMs
GENI and CloudLab provides a large-scale experiment infrastructures where users can obtain computing instances throughout the world to perform network experiments.
TCEP includes useful scripts to enable easier simulations on GENI which are described below.

```
cd scripts/
```

First, a so called RSpec XML file is needed in order to get a GENI/CloudLab cluster up and running. To automatically generate a cluster with a specified number of nodes you can execute the following command:

```
python generate_geni_rspec.py {number-of-nodes} {out-directory}
```

This will generate the rspec.xml file with the at "out-directory" with the specified number of nodes. Furthermore, this also generate the Docker swarm file (docker-swarm.cfg) with the correct amount of empty apps running on the GENI hosts.

After you deployed the RSpec on GENI, you can download a Manifest XML file which contains information of the hosts that GENI deployed. This is useful because GENI automatically generates IP addresses for the hosts and if you created a cluster with a high amount of nodes the search for this IP addresses can be a heavy overhead.
After downloading the manifest file you can run the following command to extract the IP addresses out of it and print out the config that can be put into the "docker-swarm.cfg" file:

```
python manifest_to_config.py {manifest-path}
```

This will convert the manifest and will print out the IP addresses of the started hosts in the config format for the docker deploy script.
You should see an output like this

```
manager=72.36.65.68
workers=("72.36.65.69" "72.36.65.70")
```

Now the hosts are successfully deployed on GENI and the project is ready to be setup on the hosts. To setup the GENI instances to be able to run docker, run the following command

```
./publish_tcep.sh setup
```

Note that you maybe have to enter `yes` in the console multiple times to allow SSH connections to the hosts

If you are running on a new cluster you never worked on before, you will maybe need to authorize docker on the master node to be authorized to access a private docker registry. You can do this by executing the following on the master node

```
docker login
```

After the instances are all setup, you can go forward and finally run the cluster on the hosts by executing the following command

```
./publish_tcep.sh all
```
#Running Unit Tests
Two different kinds of unit tests exist:
1. Tests for classes that do not need akka cluster -> run like other unit tests with *sbt test*
2. Tests for classes that need akka cluster -> multi-jvm setup that creates a separate jvm for each cluster node and runs them on the local machine. Run using *sbt multi-jvm:test* or *sbt multi-jvm:testOnly *classprefix**  

#### Differences to CloudLab

CloudLab is comparable to the GENI infrastructure but enables TCEP the possibility to start instances that have Docker installed already.
The steps from above are the same, except that you have to replace the name of the RSpec image to be used before calling the generate script.

Specifically, you have to replace this line in scripts/templates/rspec_node.xml 

``` 
<disk_image xmlns="http://www.geni.net/resources/rspec/3" name="urn:publicid:IDN+emulab.net+image+emulab-ops:UBUNTU16-64-STD"/>
```

with this line

``` 
<disk_image xmlns="http://www.geni.net/resources/rspec/3" name="urn:publicid:IDN+utah.cloudlab.us+image+schedock-PG0:docker-ubuntu16:0"/>
```
# CONTRAST
CONTRAST (CONtext-Feature Model-based TRAnSiTions) is an extension of TCEP[1] to make transitions between different CEP system configurations depending on changes in the network environment (context).
This is done to mitigate the performance impact of context changes on system performance, due to the fact that there is no optimal system configuration for all contexts.

For now, CEP system configurations are only defined by the active operator placement algorithm; however, adding other system mechanisms that influence performance and can be reconfigured at runtime is possible.

To find the optimal system configuration for the current context, the following steps are taken:

1. Specify the variable elements of system and context that can influence performance in a Context Feature Model
2. Collect measurement data that records the current system and context configuration (active features and attribute values) and the observed performance
3. Learn a so-called Performance Influence Model using SPLConqueror[2] (off-line, multiple linear regression learning) that determines the amount of influence each combination of features has on performance. This model can then estimate the performance of each possible system configuration for the currently given context configuration
4. At runtime, formulate the system configuration selection as an optimization problem using the Coala library [3] to transform the Performance Influence Model into the objective function to optimize for. The amount of feasible solutions (system configurations) is constrained by the CFM, and by user-formulated performance requirements
5. Execute a transition to the optimal system configuration at run-time using TransitiveCEP functionality

## Pre-requisites
* ssh-accessible remote machine(s) with Ubuntu 16.04. and 6GB+ available RAM (more simulated CEP processing nodes -> more RAM)
* Java 8 JDK on local machine
* python 2.7+
* Docker v17+
* ssh-accessible remote machine(s) with 6GB+ available RAM (more simulated CEP processing nodes -> more RAM)
* local machine: Java 8 JDK, Docker v17+, python 2.7+
* remote machines: docker and pip (should be installed by *publish_mininet.sh copy*; if installation fails, install manually and make sure docker can be run without sudo -> see https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo)
* in *application.conf* , set constants.mapek.type to "CONTRAST"
* edit *scripts/templates/docker-swarm-maki.cfg* to set registry_user to your dockerhub account, and if available, the gui server ip(machine ips do not matter, only registry_user and image names)
* Note that *publish_mininet.sh* adjusts some configuration settings in application.conf based on the parameter *nPublishers*
* set up docker to be runnable without sudo on each of the remote machines:

```
sudo groupadd docker
sudo gpasswd -a $USER docker
newgrp docker
```

## Usage

Note: the first simulation run will take a long while to set up due to downloading and building a range of docker images (especially containernet)

### Performance Influence Model Learning

1. Specify the variable context and system features as a CFM (relevant files to adjust: *cfm.cardy*,  *FmNames.scala*, *ContrastMonitor.scala*; adjust config generation in *CFM.scala:getCurrentContextConfig()* and for real-valued attributes *SPLConquerorXMLMaker.java*; update *header* variable in *publish_splc.sh* to match the header generated by *SPLCDataCollectionSimulation*).
   Capturing and calculation of context data is done by *ContrastMonitor*, which sends updates to *ContrastKnowledge*, where it is retrieved by *SPLCDataCollectionSimulation* and *ContrastAnalyzer*.
   Avoid using numeric *system* features / attributes (they greatly complicate the optimization and are currently not supported); numeric *context* features are fine.

2. Collect learning data from different system (i.e. placement algorithm) and context configurations (different network topologies, mobility simulation enabled/disabled, different workloads, etc.) over multiple simulation runs; estimate accuracy greatly depends on amount and quality of learning data.
   If you need any data from logs_backup on the remote machines that has not yet been fetched, fetch it now since the directory will be cleared.
   Run simulations on remote machines as follows (check the scripts for configuration details):
 ```
 cd scripts/mininetSim
./publish_mininet.sh compile
./publish_mininet.sh copyToAllMachines $USER
./publish_docker.sh run_all_datacollections $USER $IP
# to run specific simulations:
./publish_docker.sh run_specific_datacollections $USER $IP

# ssh command for multiple runs sometimes fails to run properly, better start directly on remote)
# to start a series of simulation runs for all algorithms directly on each remote machine:
# mobile = {True, False}
nohup python run_datacollection.py ${mobile} 'Tree' &
```
* while collecting learning data, it is advised to disable transitions (*application.conf -> constants.mapek.transitions-enabled = false*) to exclude transition-related side effects from the learning data.
* learning data from previous simulations is moved to the folder logs_backup with a timestamp

3 . Move the collected data into the *splc/datacollection* folder via ```./scripts/mininetSim/publish_mininet.sh fetch_datacollection $USER ```
* if the feature model has been changed, make sure the header variable in scripts/publish_splc.sh:36 matches the header of the _splc.csv files
* Then run *./splc/publish_splc.sh learn $USER $IP* (preferably on a fast VM, this can take some time depending on the amount of data)

  This combines all the data in splc/datacollection and starts the learning scripts (learnLatency.a, learnLoad.a, ...) with SPLConqueror in a docker container.
  The learning process can be configured in *MLSettings.txt* (see SPLC git repository for details)

  The .log files produced by SPLC contain the settings of the learning process and its results, the Performance Influence Models.
  For a commented example logfile, see *splc/example/commented_example.log*.
  Varying the learning settings (*MLSettings.txt*) can lead to different models with differences in the learning error.

* Move the learned performance influence models (latencyModel.log, ...) from the *splc/learningLogs* folder on the remote machine into *src/main/resources/performanceModels* by calling *./scripts/publish_splc.sh fetch $USER $IP* once all containers have exited.

* Finding a set of features where each features has a significant influence is not always easy. Since collecting measurement data takes some time, it may be a good idea to define more (candidate) context features, and let SPLC determine which of them have significant influence (note however, that more features means more necessary (diverse) learning data and learning time; a balance has to be found here). If a feature is found to have influence, it may be worth investigating whether this features sufficiently describes the interaction, or if it can be split into (two or more) more specific features.
  Automating the execution of data collection simulations (e.g. using MACI) could ease this process greatly.

### Context-based Transitions

To run a simulation with context changes that trigger a placement algorithm transition, call
```
# mininet simulation setup
./scripts/mininetSim/publish_mininet.sh all $USER $IP $DURATION(MINUTES) $STARTING_ALGORITHM $BASE_LINK_LATENCY $MOBILITY_ENABLED $NETWORK_TOPOLOGY
# e.g. ./publish_docker.sh all foo 1.2.3.4 15 Pietzuch 30 True Tree

# or geni simulation setup (after setting up geni with manifest_to_rspec.py)
./scripts/publish_tcep.sh all
```


* Simulation will start after setup (can take a few minutes); simulation of publisher mobibilty starts after an interval specified in *mininet_wired_tree.py*
* depending on the performance estimates made by the latency performance model (latencyModel.log), the system will adjust the active placement algorithm depending on the current context. The current Coala version does not allow combining multiple Performance Models, but an upgrade is possible
* user QoS requirements are taken into account when making transitions: if the optimal system configuration is estimated (using the performance model for the respective requirement metric) to violate the requirement, the transition is not carried out, and the system configuration is excluded via an additional constraint on the CFM (*ContrastPlanner*, *RequirementChecker*)

Further notes:
* the script will launch a containernet[5] docker container on the remote host, which in turn starts the mininet simulation (mininet_wired_tree.py)
* the mininet simulation then starts docker containers pertaining to publishers (*PublisherApp*), subscribers (*SimulationRunner*) and brokers (*EmptyApps*) corresponding to the query described in the simulation. Note that these containers are not started inside the containernet container, but bind to the host's docker socket
* use docker ps -a to list all docker containers and their status
* use docker exec -it \<name\> bash to shell into the docker containers
* run tail -f "adaptiveCEP.log" to see live logs
* use docker rm -f  $(docker ps -a -q) to shut down all running containers
* the results of the simulation run can be found in ~/logs_backup/\*/simulation/*_splc.csv on the remote machine

### Troubleshooting
* check if all docker images have been built correctly, especially containernet. Try running one of the sample mininet simulations located in the *examples* folder inside a containernet container to make sure mininet works properly.
* on the first run, all necessary files will be copied over to the remote machine, and the docker containers will be created. This can take a (long) while. Make sure that all containers are built without errors, and ~/madrid_traces/short_traces.tar.gz is unpacked successfully.
* in case of SPLConqueror printing 'Invalid configuration' lines in the .error file: check if the attribute values are all within the attribute boundaries in the supplied cfm.xml / cfm.cardy file! Also check if the step function in the cfm.xml file matches the rounding of the measurements
* if SPLC has problems loading measurements, be sure to check if measurement csv files have correct separators (;) and line breaks (\n)
* make sure your IDE does not sort python import statements (mininet_wired_tree.py otherwise has a problem with cyclic mininet dependencies (see https://github.com/mininet/mininet/issues/546))
  In Intellij idea: File->Settings->Editor->CodeStyle->Python->sort imports checkbox
* if simulation does not start due to single publisher not joining, simply restart simulation by running *./publish_mininet.sh publish user host ...* again



This project is based on <a href="https://pweisenburger.github.io/AdaptiveCEP/" target="_blank">AdaptiveCEP</a> for specifying complex events and QoS demands. 

## [Publications](#publications)

+ M. Luthra, B. Koldehofe, R. Arif, P. Weisenburger, G. Salvaneschi, TCEP: Adapting to Dynamic User Environments by Enabling Transitions between Operator Placement Mechanisms. In Proceedings of the 12th ACM International Conference on Distributed and Event-based Systems (DEBS ’18), pp. 136–147. <a href="https://doi.org/10.1145/3210284.3210292" target="_blank">10.1145/3210284.3210292</a>
+ P. Weisenburger, M. Luthra, B. Koldehofe and G. Salvaneschi, Quality-Aware Runtime Adaptation in Complex Event Processing. In IEEE/ACM 12th International Symposium on Software Engineering for Adaptive and Self-Managing Systems (SEAMS '17), pp. 140-151. <a href="https://doi.org/10.1109/SEAMS.2017.10" target="_blank">10.1109/SEAMS.2017.10</a>
+ M. Luthra, S. Hennig, B. Koldehofe, Understanding the Behavior of Operator Placement Mechanisms on Large Scale Networks. In the Proceedings of 19th ACM/IFIP/USENIX International Middleware Conference 2018: Posters and Demos (Middleware 2018), pp. 19-20. <a href="https://doi.org/10.1145/3284014.3284024" target="_blank">10.1145/3284014.3284024</a>
+ M. Luthra, B. Koldehofe, R. Steinmetz, Transitions for Increased Flexibility in Fog Computing: A Case Study on Complex Event Processing. Informatik Spektrum 42, pp. 244–255 (2019). <a href="https://doi.org/10.1007/s00287-019-01191-0" target="_blank">10.1007/s00287-019-01191-0</a>
+ M. Luthra and B. Koldehofe, ProgCEP: A Programming Model for Complex Event Processing over Fog Infrastructure. In Proceedings of the 2nd International Workshop on Distributed Fog Services Design @ Middleware Conference (DFSD ’19@Middleware), pp. 7-11. <a href="https://doi.org/10.1145/3366613.3368121" target="_blank">10.1145/3366613.3368121</a>


## [Acknowledgement](#acknowledgement)

This work has been co-funded by the German Research Foundation (DFG) within the <a href="https://www.maki.tu-darmstadt.de/sfb_maki/ueber_maki/index.en.jsp" target="_blank">Collaborative Research Center (CRC) 1053 -- MAKI</a>

## [Contact](#contact)

Feel free to contact <a href="https://www.kom.tu-darmstadt.de/kom-multimedia-communications-lab/people/staff/manisha-luthra/" target="_blank">Manisha Luthra</a> or <a href="https://www.rug.nl/staff/b.koldehofe/" target="_blank">Boris Koldehofe</a> for any questions. 


