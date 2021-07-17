#!/usr/bin/python

"""
***Requirements***:

sumo 1.5.0
sumo-gui"""

import datetime
import os
import os.path
import re
import subprocess
import sys
import time
from mininet.link import TCULink
from mininet.log import setLogLevel, info
from mininet.node import RemoteController
from mininet.util import macColonHex
from mn_wifi.net import Mininet_wifi

from mn_wifi.link import TCWirelessLink
from mn_wifi.node import UserAP
from mn_wifi.sumo.runner import sumo


def topology(enable_tcep=True):
    rsu_range = 2.6  # smaller value -> larger range

    "Create a network."
    # change ipBase from /8 to /24 to avoid trouble with KOM IP (10.2.1.*)
    net = Mininet_wifi(accessPoint=UserAP, link=TCWirelessLink, ipBase='20.0.0.0/8')
    net.setAssociationCtrl(ac='ssf')
    rsus = []
    aps = []

    info("*** Creating nodes\n")
    for i in range(0, n_publishers):
        net.addCar('car%s' % (i+1), wlans=1, position='-10000,-10000,0')

    chan = 0
    for i in range(0, 12):
        pos = '%i,%i,0' % (1000 + i * 500, 1000)
        ap = net.addAccessPoint('ap%i' % (i+1), ssid='vanet-ssid', mac=macColonHex(i+1),
                                mode='n', channel=str((chan % 10) + 1),
                                ieee80211r='yes', mobility_domain='a1b2',
                                listenPort=(i+6000),
                                passwd='123456789a', encrypt='wpa2',
                                position=pos)
        aps.append(ap)
        chan = chan + 5  # avoid interfering channels in overlapping rsus -> rotate through channels 1, 6, 11
        # add a worker node (road-side-unit / rsu) at each section ap
        rsu = net.addHost('rsu%i' % (i+1), position='0.0,0.0,0.0')
        rsus.append(rsu)

    prediction_server = None
    if mapek == "ExchangeablePerformanceModel":
        prediction_server = net.addHost('h1', ip='20.0.0.250')
        #some_cmd > some_file 2>&1 &
        prediction_server.cmd("pkill -f 'run_prediction_endpoint*'")
        prediction_server.cmd("python3 run_prediction_endpoint.py -l %s -t %s > ../logs/predictionServerConsole.log 2>&1 &" %
                              (latency_model, throughput_model))
        info("started prediction endpoint on 20.0.0.250")

    c1 = net.addController(name='onos', controller=RemoteController, ip=controller_ip, port=6653)
    #c1 = net.addController()
    # manually stop network-manager since it interferes with wpa_cli association
    aps[4].cmd('pkill -f NetworkManager')
    aps[4].cmd('pkill -f tcep.machinenodes')
    aps[4].cmd('pkill -f tcep.simulation')

    info("*** Configuring Propagation Model\n")
    net.setPropagationModel(model="logDistance", exp=rsu_range)

    info("*** Configuring wifi nodes\n")
    net.configureWifiNodes()

    info("*** Starting SUMO road traffic simulation, gui enabled:", enable_gui, "\n")
    os.environ["SUMO_HOME"] = "/usr/share/sumo/"
    net.useExternalProgram(program=sumo, port=8813,
                           config_file='osm.sumocfg', stations=rsus, sumo_gui=enable_gui)

    time.sleep(5.0)  # wait for AP positions
    # TCLink makes these wired (eth) links between APs
    latency = '3ms'
    for i in range(0, 9):
        net.addLink(aps[i], aps[i + 1], cls=TCULink, delay=latency)

    net.addLink(aps[10], aps[4], cls=TCULink, delay=latency)
    net.addLink(aps[4], aps[11], cls=TCULink, delay=latency)
    if prediction_server is not None:
        net.addLink(aps[4], prediction_server, cls=TCULink)

    for i in range(0, len(rsus)):
        info('connecting %s to %s\n' % (rsus[i], aps[i]))
        net.addLink(aps[i], rsus[i], cls=TCULink, delay='1ms')

    info("\n\n*** Starting network\n")
    net.build()
    # add NAT to mininet that is reachable under 20.0.0.254 and forwards all packets to outside world
    gui_subnet = '%s/32' % gui_ip
    net.addNAT(name='nat0', linkTo='ap5', ip='20.0.0.254', net=gui_subnet).configDefault()
    c1.start()

    for ap in aps:
        ap.start([c1])
    # set IPs for cars and rsus
    # add route to KOM VMs to all nodes
    for car in net.cars:
        car.setIP('20.0.0.%s/24' % (int(net.cars.index(car))+101), intf='%s-wlan0' % car.name)
        car.cmd('ip route add %s via 20.0.0.254' % gui_subnet)
        car.cmd('export PATH=/opt/ibm/java-x86_64-80/bin:$PATH CPLEX_LIB_PATH=%s/tcep/cplex/' % homedir)

    for i in range(0, len(rsus)):
        rsus[i].setIP('20.0.0.%s/24' % (i + 11))
        rsus[i].cmd('ip route add %s via 20.0.0.254' % gui_subnet)
        rsus[i].cmd('export PATH=/opt/ibm/java-x86_64-80/bin:$PATH CPLEX_LIB_PATH=%s/tcep/cplex/' % homedir)

    time.sleep(15.0)
    info('*** checking if wpa_supplicant was started successfully (sometimes fails to start for reasons unknown)\n')
    for car in net.cars:
        wpa_supplicant_running = True#car.cmd('pgrep -f %s_0.staconf' % car.wintf[0])
        if not wpa_supplicant_running:
            info('\n****ERROR: wpa_supplicant was NOT started on %s, stopping the simulation now. Restarting the simulation usually fixes this \n' % car)
            net.stop()
            return
        else:
            info('wpa_supplicant running on %s: pid %s\n' % (car.name, wpa_supplicant_running))

        # add this call here to force association with correct ap (and stop wpa_supplicant from switching between first aps in scan list)
        ap = car.wintfs[0].associatedTo
        assert ap is not None, "%s is not associated to any AP" % car.wintfs[0]
        info('calling wpa_cli -i %s roam %s to force wpa association of %s to initial ap %s\n' % (car.wintfs[0].name, ap.wintfs[0].mac, car, ap))
        for i in range(10):
            subprocess.call(['util/m', car.name, 'wpa_cli', '-i %s' % car.wintfs[0].name, 'roam %s' % ap.wintfs[0].mac])
            time.sleep(0.05)
        subprocess.call(['util/m', car.name, 'wpa_cli', '-i %s' % car.wintfs[0].name, 'roam %s' % ap.wintfs[0].mac])

    # Draw the network and write traces to files
    if enable_gui:
        nodes = net.cars + net.aps
        net.telemetry(nodes=nodes, data_type='position',
                      min_x=0, min_y=0,
                      max_x=11500, max_y=3500)

    if enable_tcep:
        info("*** %s Starting TCEP on cars and RSUs \n" % datetime.datetime.now())

        COMMON_CONFIG = '-XX:+UseParallelGC -XX:+UseNUMA -Xmx2048m  ' \
                        '-XX:+HeapDumpOnOutOfMemoryError -DIBM_HEAPDUMP=true -DIBM_HEAPDUMP_OUTOFMEMORY=true ' \
                        '-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.local.only=false ' \
                        '-Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false ' \
                        '-Djava.rmi.server.hostname=localhost '
        LOG_PATH = '%s/tcep/logs' % homedir

        publisher_kind = "LinearRoadPublisher" if query == "LinearRoad" else "SpeedPublisher"
        # start publishers on cars
        for i in range(0, n_publishers):
            ARGS = '--dir %s/p%i --ip %s --port %i --numberOfPublishers %i --kind %s --eventRate %s ' \
                   % (LOG_PATH, i+1, net.cars[i].IP(), publisher_base_port+i, i+1, publisher_kind, eventrate)
            net.cars[i].cmd('java %s -DlogFilePath=%s/p%i '
                            '-DIBM_HEAPDUMPDIR="%s/p%i" '
                            '-Dcom.sun.management.jmxremote.port=%i '
                            '-Dcom.sun.management.jmxremote.rmi.port=%i '
                            '-DMAIN=tcep.machinenodes.PublisherApp '
                            '-cp %s "tcep.machinenodes.PublisherApp" %s > /dev/null &'
                            % (COMMON_CONFIG, LOG_PATH, i+1, LOG_PATH, i+1, 8484+i+1, 8484+i+1, JARFILE, ARGS))
            info("*** %s Starting TCEP speed PublisherApp on %s with ARGS %s \n" % (datetime.datetime.now(), net.cars[i], ARGS))

        # start workers on RSUs
        ap_to_rsu_ratio = len(aps) / float(n_rsus)
        for i in range(0, n_rsus):
            rsu_index = int(i * ap_to_rsu_ratio) % len(aps) if n_rsus < len(aps) else i % len(aps)

            ARGS = '--dir %s/n%i --ip %s --port %i --eventRate %s' \
                   % (LOG_PATH, i+1, rsus[rsu_index].IP(), base_port-i-1, eventrate)
            rsus[rsu_index].cmd('java %s -DlogFilePath=%s/n%i '
                                '-DIBM_HEAPDUMPDIR="%s/n%i" '
                                '-Dcom.sun.management.jmxremote.port=%i '
                                '-Dcom.sun.management.jmxremote.rmi.port=%i '
                                '-DMAIN=tcep.machinenodes.EmptyApp '
                                '-cp %s "tcep.machinenodes.EmptyApp" %s > /dev/null &'
                                % (COMMON_CONFIG, LOG_PATH, i+1, LOG_PATH, i+1, 8484-i-1, 8484-i-1, JARFILE, ARGS))
            info("*** %s Starting TCEP EmptyApp on %s with ARGS %s \n" % (datetime.datetime.now(), rsus[rsu_index], ARGS))

        sim_rsu = rsus[4]  # the rsu at the intersection of the two main roads in the SUMO sim, IP: 20.0.0.15 hosts densityPublisher and simulationRunner

        if query == "AccidentDetection":
            ARGS = '--dir %s/dp%i --ip %s --port %i --kind DensityPublisher --eventRate %s' \
                   % (LOG_PATH, 1, sim_rsu.IP(), publisher_base_port + n_publishers, eventrate)
            info("*** %s Starting TCEP DensityPublisher on %s with ARGS %s\n" % (datetime.datetime.now(), sim_rsu, ARGS))
            sim_rsu.cmd('java %s -DlogFilePath=%s/dp%i '
                        '-DIBM_HEAPDUMPDIR="%s/dp%i" '
                        '-Dcom.sun.management.jmxremote.port=%i '
                        '-Dcom.sun.management.jmxremote.rmi.port=%i '
                        '-DMAIN=tcep.machinenodes.PublisherApp '
                        '-cp %s "tcep.machinenodes.PublisherApp" %s > /dev/null &'
                        % (COMMON_CONFIG, LOG_PATH, 1, LOG_PATH, 1, 8484 + n_publishers + 2, 8484 + n_publishers + 2, JARFILE, ARGS))

        mode_id = 14 if query == "LinearRoad" else 5
        ARGS = '--dir %s/simulation --mode %s --ip %s --port %i --duration %i ' \
               '--initialAlgorithm %s --numberOfPublishers %i --req %s --query %s --mapek %s --transitionStrategy %s --transitionExecutionMode %s ' \
               '--eventRate %s ' \
               % (LOG_PATH, mode_id, sim_rsu.IP(), base_port, duration, placement, n_publishers, reqChange, query, mapek, transitionStrategy, transitionExecutionMode, eventrate)
        info("*** %s Starting TCEP SimulationRunner on %s with ARGS %s\n" % (datetime.datetime.now(), sim_rsu, ARGS))
        # rsu5 LOG_FILE_PATH='"/home/niels/tcep"' ; MAIN='"tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar"'; ARGS='"--dir ${LOG_FILE_PATH}/simulation --mode 9 --ip 20.0.0.15 --port 2500 --duration 5 --initialAlgorithm MDCEP --numberOfPublishers 12 "'; java -DMAIN="tcep.simulation.tcep.SimulationRunner" -DlogFilePath=${LOG_FILE_PATH}/simulation -cp ${MAIN} ${ARGS}
        sim_rsu.cmd('java %s -DlogFilePath=%s/simulation '
                    '-DIBM_HEAPDUMPDIR="%s/simulation" '
                    '-Dcom.sun.management.jmxremote.port=%i '
                    '-Dcom.sun.management.jmxremote.rmi.port=%i '
                    '-DMAIN=tcep.simulation.tcep.SimulationRunner '
                    '-cp %s "tcep.simulation.tcep.SimulationRunner" %s > /dev/null &'
                    % (COMMON_CONFIG, LOG_PATH, LOG_PATH, 8484, 8484, JARFILE, ARGS))

        consumer_kind = "Toll" if query == "LinearRoad" else "Accident"
        ARGS = '--dir %s/consumer --ip %s --port %i --kind %s --eventRate %s' \
               % (LOG_PATH, sim_rsu.IP(), base_port + 200, consumer_kind, eventrate)
        info("*** %s Starting TCEP Consumer on %s with ARGS %s\n" % (datetime.datetime.now(), sim_rsu, ARGS))
        sim_rsu.cmd('java %s -DlogFilePath=%s/consumer '
                    '-DIBM_HEAPDUMPDIR="%s/consumer" '
                    '-Dcom.sun.management.jmxremote.port=%i '
                    '-Dcom.sun.management.jmxremote.rmi.port=%i '
                    '-DMAIN=tcep.machinenodes.ConsumerApp '
                    '-cp %s "tcep.machinenodes.ConsumerApp" %s > /dev/null &'
                    % (COMMON_CONFIG, LOG_PATH, LOG_PATH, 8484 + 200, 8484 + 200, JARFILE, ARGS))


    #info("*** Running CLI\n")
    #CLI(net)
    time.sleep((duration + 1) * 60)
    info("*** Stopping network\n")
    aps[4].cmd('pkill -f tcep.machinenodes')
    aps[4].cmd('pkill -f tcep.simulation')
    net.stop()


if __name__ == '__main__':
    assert os.path.exists('/opt/ibm/java-x86_64-80/bin')
    setLogLevel('info')
    info("python args: %s" % sys.argv)
    # global constants
    # duration = simulation duration in minutes
    # initial_algorithm = CEP placement algorithm to start with
    # mobility_enabled = enable or disable publisher mobility simulation
    # link_latency = base link latency between switches
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    placement = sys.argv[2] if len(sys.argv) > 2 else 'ProducerConsumer'
    n_publishers = int(sys.argv[3]) if len(sys.argv) > 3 else 12
    user = str(sys.argv[4]) if len(sys.argv) > 4 else 'niels' # this returns 'root' since mininet must run with sudo: pwd.getpwuid(os.getuid())[0]
    enable_gui = sys.argv[5].lower() == 'true' if len(sys.argv) > 5 else True
    controller_ip = sys.argv[6] if len(sys.argv) > 6 else 'localhost'
    n_rsus = int(sys.argv[7]) if len(sys.argv) > 7 else 4
    gui_image = sys.argv[8] if len(sys.argv) > 8 else 'nieda2018/tcep-gui'
    mapek = sys.argv[9] if len(sys.argv) > 9 else 'requirementBased'
    query = sys.argv[10] if len(sys.argv) > 10 else 'AccidentDetection'
    reqChange = sys.argv[11] if len(sys.argv) > 11 else 'hops'
    transitionStrategy = sys.argv[12] if len(sys.argv) > 12 else 'MFGS'
    transitionExecutionMode = int(sys.argv[13]) if len(sys.argv) > 13 else 1
    eventrate = sys.argv[14] if len(sys.argv) > 14 else "100"
    gui_ip = sys.argv[15] if len(sys.argv) > 15 else "172.30.0.254"
    latency_model = sys.argv[16] if len(sys.argv) > 16 else "tpot_trained_pipeline_8_mininet_accident_1s_combined_samples.csv_processingLatencyMean.joblib"
    throughput_model = sys.argv[17] if len(sys.argv) > 17 else "autosklearn_trained_pipeline_8_mininet_accident_1s_combined_samples.csv_eventRateOut.joblib"
    latency_model = latency_model.split("/")[-1]
    throughput_model = throughput_model.split("/")[-1]
    homedir = '/home/' + user
    JARFILE = homedir + '/tcep/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar'
    with open(homedir + "/tcep/application.conf") as f:
        base_port = int(re.compile(r'base-port = \d+').search(f.read()).group(0).split(' = ')[1])

    publisher_base_port = base_port + 1

    topology(enable_tcep=True)
