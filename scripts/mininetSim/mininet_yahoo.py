#!/usr/bin/env python2

import datetime
import logging
import sys
import time
from apscheduler.schedulers.background import BackgroundScheduler
from collections import OrderedDict
from mininet.node import Controller
from mininet.link import TCLink
from mininet.log import setLogLevel, info, debug
from mininet.net import Containernet
from mobility import MobilitySwitch, moveHost
from mininet.cli import CLI

import traceLoader as tl

simMode = 15 # 9 - general thesis | 5 - SPLC/CONTRAST / 10 - Accident / 11 - LinearRoad / 12 - Yahoo


# create a CEP processing node and add it to the mininet topology
def create_processing_node(topology, name, ip, port):
    jmx_port = int(port) + 1000
    n = topology.addDocker(
        name, dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.machinenodes.EmptyApp",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--ip " + str(ip) + " --port " + str(port), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["/home/" + str(user) + "/logs/" + str(name) + ":/app/logs"],
        ports=[port, jmx_port],
        port_bindings={port: port, jmx_port:jmx_port})
    n.cmd('nohup ./docker-entrypoint.sh &')
    return n

def create_consumer(topology, name, ip, port, kind):
    jmx_port = int(port) + 1000
    n = topology.addDocker(
        name, dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.machinenodes.ConsumerApp",
                    "LOG_FILE_PATH=/app/logs",
                     "ARGS=--ip " + str(ip) + " --port " + str(port) + " --kind " + str(kind), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["/home/" + str(user) + "/logs/" + str(name) + ":/app/logs",
                 "/home/" + str(user) + "/tcep/mobility_traces:/app/mobility_traces"],
        ports=[port, jmx_port],
        port_bindings={port: port, jmx_port:jmx_port}
    )
    n.cmd("nohup ./docker-entrypoint.sh &")
    return n

# create a CEP publisher and add it to the mininet topology
# kind: SpeedPublisher (others can be defined in PublisherApp.scala)
# p1 = create_publisher(topology=net, name='p1', ip='10.0.0.101', port='2520', kind='SpeedPublisher', id=1)
def create_publisher(topology, name, ip, port, kind, id):
    jmx_port = int(port) + 1000
    p = topology.addDocker(
        name, dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.machinenodes.PublisherApp",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--ip " + str(ip) + " --port " + str(port) + " --kind " + str(
                         kind) + " --numberOfPublishers " + str(id), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["/home/%s/logs/%s:/app/logs" % (user, name),
                 "/home/%s/tcep/mobility_traces:/app/mobility_traces" % user],
        ports=[port, jmx_port], port_bindings={port: port, jmx_port:jmx_port})
    p.cmd('nohup ./docker-entrypoint.sh &')
    return p


################################
# setup and run the simulation #
################################
def simulation(simDuration, initial_algorithm, baseLatency, image, num_publishers=8, enable_cli=False, combinedPIM=0):
    delaystr = str(baseLatency) + 'ms'
    baseBandwidth = 100  # bandwidth on links to simulate by mininet
    ### mobility tracefile related settings
    setupDelay = 60  # s; time to wait before starting the simulation in seconds
    
    net = Containernet(controller=Controller, switch=MobilitySwitch)
    info('*** Adding controller\n')
    c0 = net.addController('c0')

    info('*** Adding docker containers\n')
    publishersdict = {}
    leaf_switches = []
    for i in range(0, num_publishers):
        p = create_publisher(topology=net, name='p%i' % (i + 1), ip='10.0.0.%i' % (101 + i), port='%i' % (2520 + i),
                             kind='YahooPublisher', id=(i + 1))
        publishersdict[i + 1] = p
        s = net.addSwitch(name='s%i' % i)
        net.addLink(p, s) # TCLink has no effect between switch and host
        leaf_switches.append(s)

    nodes = []
    switches, upper_layer_switches, intermediate_layer_switches, nodes, max_hops = create_topology(net, leaf_switches,
                                                                                          nodes, baseLatency,
                                                                                          baseBandwidth)

    sub = net.addDocker(
        name='simulator', dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.simulation.tcep.SimulationRunner",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--dir ./logs --mode " + str(simMode) + " --ip 10.0.0.253 --port 2500 --duration " + str(simDuration) + \
                     " --initialAlgorithm " + str(initial_algorithm) +
                     " --baseLatency " + str(baseLatency) +
                     " --numberOfPublishers " + str(num_publishers) +
                     " --maxPubToClientHops " + str(max_hops) +
                     " --combinedPIM " + str(combinedPIM), "JMX_PORT=8484"],
        ip='10.0.0.253',
        volumes=["/home/" + str(user) + "/logs/simulation:/app/logs",
                 "/home/" + str(user) + "/tcep/handovers.csv:/app/handovers.csv"],
        ports=[2500, 8484, 5266], port_bindings={2500: 2500, 8484:8484, 5266:5266})
    sub.cmd('nohup ./docker-entrypoint.sh &')
    nodes.append(sub)
    sub_switch = net.addSwitch('s%i' % (len(switches)+len(leaf_switches)))
    switches.append(sub_switch)
    net.addLink(sub_switch, sub)
    for s in upper_layer_switches:
        net.addLink(sub_switch, s, cls=TCLink, delay=delaystr, bw=baseBandwidth)
    consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="AdAnalysis")
    #consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="Toll")
    #consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="Accident")
    #consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="AvgSpeed")
    consumer_switch = net.addSwitch('s%i' % (len(switches)+len(leaf_switches)))
    net.addLink(consumer_switch, consumer)
    net.addLink(consumer_switch, sub_switch, cls=TCLink, delay=delaystr, bw=baseBandwidth)
    publisherInSection = OrderedDict()

    ###############################################
    # SIMULATION START                            #
    ###############################################
    info('*** Starting network\n')
    net.start()

    start_time = time.time()
    time_passed = time.time() - start_time + 1
    if (time_passed < setupDelay):
        print "waiting " + str(setupDelay - time_passed) + "s before starting ..."
        time.sleep(setupDelay - time_passed)

    # net.pingAllFull() # -> test reachability
    if(enable_cli):
        info('*** Running CLI\n')
        CLI(net)

    durationSeconds = int(simDuration) * 60
    time.sleep(durationSeconds + setupDelay)
    info('*** Stopping network')
    # for pub in jobs:
    #    jobs[pub].remove()
    sched.shutdown()
    net.stop()


def create_topology(net, publishers, nodes, lat, bw):
    # connect the given list of publishers
    # create (2-3) layered tree hierarchy with publishers/cars as leaves and processing nodes at nodes
    # each node has 2-3 children
    switches_l1 = []  # road-side units / switches connecting to publishers
    switches_l2 = []  # layer connecting road-side switches
    info('*** creating wired tree topology with %i publishers\n' % len(publishers))
    for i in range(0, 4):
        offset = len(publishers)+i
        s = net.addSwitch(name="s%i" % offset)
        switches_l1.append(s)
        n = create_processing_node(net, "n%i" % (offset-len(publishers)+1), ip="10.0.0.%i"%(10+offset-len(publishers)), port=2520+offset)
        nodes.append(n)
        net.addLink(n,s)
        net.addLink(s, publishers[i*2+0], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, publishers[i*2+1], cls=TCLink, delay=lat, bw=bw)
    '''
    for i in range(0, 2):
        offset = len(publishers)+i
        s = net.addSwitch(name="s%i" % offset)
        switches_l1.append(s)
        n = create_processing_node(net, "n%i" % (offset-len(publishers)+1), ip="10.0.0.%i"%(10+offset-len(publishers)), port=2520+offset)
        nodes.append(n)
        net.addLink(n,s)
        net.addLink(s, publishers[i*4+0], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, publishers[i*4+1], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, publishers[i*4+2], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, publishers[i*4+3], cls=TCLink, delay=lat, bw=bw)
    '''
    
    for i in range(0, 2):
        offset = len(publishers)+len(switches_l1)+i
        s = net.addSwitch(name="s%i" % offset)
        switches_l2.append(s)
        n = create_processing_node(net, "n%i" % (offset-len(publishers)+1), ip="10.0.0.%i" % (10 + offset-len(publishers)), port=2520+offset)
        nodes.append(n)
        net.addLink(s, switches_l1[2*i], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, switches_l1[2*i+1], cls=TCLink, delay=lat, bw=bw)
        net.addLink(n, s)
    max_hops = 3
    return switches_l1+switches_l2, switches_l2, switches_l1, nodes, max_hops


if __name__ == '__main__':
    setLogLevel('info')

    # duration = simulation duration in minutes
    # initial_algorithm = CEP placement algorithm to start with
    # mobility_enabled = enable or disable publisher mobility simulation
    # link_latency = base link latency between switches
    duration = sys.argv[1] if len(sys.argv) > 1 else 10
    placement = sys.argv[2] if len(sys.argv) > 2 else 'MDCEP'
    latency = sys.argv[3] if len(sys.argv) > 3 else 10
    user = sys.argv[4] if len(sys.argv) > 4 else 'blins'
    image = str(sys.argv[5]) + '/tcep' if len(sys.argv) > 5 else 'nzumbe/tcep'
    nPublishers = int(sys.argv[6]) if len(sys.argv) > 6 else 8
    combinedPIM = int(sys.argv[6]) if len(sys.argv) > 7 else 0
    simulation(simDuration=duration, initial_algorithm=placement, baseLatency=latency,
               image=image, num_publishers=nPublishers, enable_cli=False, combinedPIM=combinedPIM)
