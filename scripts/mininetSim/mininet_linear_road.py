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

simMode = 14 # 9 - general thesis | 5 - SPLC/CONTRAST / 10 - Accident / 11 - LinearRoad


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
        volumes=["/home/" + str(user) + "/logs/" + str(name) + ":/app/logs"],
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
                         kind) + " --numberOfPublishers " + str(id) + " --eventWaitTime " + str(30), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["/home/%s/logs/%s:/app/logs" % (user, name),
                 "/home/%s/tcep/mobility_traces:/app/mobility_traces" % user],
        ports=[port, jmx_port], port_bindings={port: port, jmx_port:jmx_port})
    p.cmd('nohup ./docker-entrypoint.sh &')
    return p

# simulate node mobility by removing the link to the old switch and creating a new link to the new switch
def moveNodeToSwitch(net, node, oldSwitch, newSwitch, delay, bw):
    info("*** Moving %s from %s to %s \n" % (node, oldSwitch, newSwitch))
    debug("connections from %s to %s: %s \n" % (node, oldSwitch, node.connectionsTo(oldSwitch)))
    print "connections from %s to %s: %s" % (node, oldSwitch, node.connectionsTo(oldSwitch))
    hintf, sintf = moveHost(node, oldSwitch, newSwitch)
    delaystr = str(delay)
    hintf.config(bw=bw, delay=delaystr)
    sintf.config(bw=bw, delay=delaystr)
    info("*** Clearing out old flows with IP %s \n" % node.IP())
    kwargs = { 'verbose' : True }
    for sw in net.switches:

        sw.dpctl('del-flows', 'ip,nw_src=%s' % node.IP())
        sw.dpctl('del-flows', 'ip,nw_dst=%s' % node.IP())
        sw.dpctl('del-flows', 'arp,nw_src=%s' % node.IP())
        sw.dpctl('del-flows', 'arp,nw_dst=%s' % node.IP())  # must explicitly state dl_type (arp/ip), otherwise ip is ignored

        #sw.dpctl('dump-flows', 'ip,nw_src=%s' % node.IP())
        #sw.dpctl('dump-flows', 'ip,nw_dst=%s' % node.IP())
        #sw.dpctl('dump-flows', 'arp,nw_src=%s' % node.IP())
        #sw.dpctl('dump-flows', 'arp,nw_dst=%s' % node.IP())  # must explicitly state dl_type (arp/ip), otherwise ip is ignored

    #debug("\n*** dump of old switch %s after delete \n" % oldSwitch)
    #oldSwitch.dpctl('dump-flows')
    return hintf, sintf


# attempt to add new node at runtime, does not work properly
#def addDummyNodeAtRuntime(net, switchName, nodeName, ip, port):
#    switch, node = net.get(switchName), create_processing_node(topology=net, name=nodeName, ip=ip, port=port)
#    nlink = TCLink(node, switch)
#    switch.attach(nlink.intf1)
    # s1, s4, h3 = net.get( 's1' ), net.addSwitch( 's4' ), net.addHost( 'h3' )
    # hlink, slink = TCLink( h3, s4 ), TCLink( s1, s4 )
    # Configure and start up
    # s1.attach( slink.intf1 )
    # s4.start( net.controllers )
    # h3.configDefault( defaultRoute=h3.defaultIntf() )


################################
# setup and run the simulation #
################################
def simulation(simDuration, initial_algorithm, baseLatency, mobility_enabled, image, num_publishers=8,
               publishers_per_section=1, enable_cli=False, combinedPIM=0):
    delaystr = str(baseLatency) + 'ms'
    baseBandwidth = 100  # bandwidth on links to simulate by mininet
    ### mobility tracefile related settings
    setupDelay = 100  # s; time to wait before starting the simulation in seconds
    clockInterval = 1.0  # s
    checkInterval = 5.0  # s; update interval for checking if a publisher needs to be moved to the next switch
    stopAtHalf = False  # insert a 5-minute period where publisher mobility is disabled after half the simulation / at the start
    stopAtStart = True  # !!!! DO NOT start moving publishers around before akka cluster is up !!!
    # dictionary with keys 1 to sections * vehiclesPerSection,
    # values trace data frame (time, position, speed) for each vehicle
    traces = tl.load_linear_road_traces('mobility_traces/', 30, nPublishers)#tl.load_linear_road_traces(num_publishers, trace_file='mobility_traces/linearroad_data_preprocessed.csv', stop_at_start=stopAtStart)
    
    #num_sections = int(num_publishers / publishers_per_section)
    num_sections = 6 # How many road side units
    
    net = Containernet(controller=Controller, switch=MobilitySwitch)
    info('*** Adding controller\n')
    c0 = net.addController('c0')

    info('*** Adding docker containers\n')
    publishersdict = {}
    leaf_switches = []
    for i in range(0, num_publishers):
        p = create_publisher(topology=net, name='p%i' % (i + 1), ip='10.0.0.%i' % (101 + i), port='%i' % (2520 + i),
                             kind='LinearRoadPublisher', id=(i + 1))
        publishersdict[i + 1] = p
        s = net.addSwitch(name='s%i' % i)
        net.addLink(p, s) # TCLink has no effect between switch and host
        leaf_switches.append(s)

    nodes = []
    switches, upper_layer_switches, intermediate_layer_switches, nodes, max_hops = create_topology(net, leaf_switches,
                                                                                          nodes, baseLatency,
                                                                                          baseBandwidth)
    #SECTIONS = [i for i in range(45,57)]
    SECTIONS = [i for i in range(47,53)]
    '''section_to_rsu = {
        SECTIONS[0] : 0,
        SECTIONS[1] : 0,
        SECTIONS[2] : 1,
        SECTIONS[3] : 1,
        SECTIONS[4] : 2,
        SECTIONS[5] : 2,
        SECTIONS[6] : 3,
        SECTIONS[7] : 3,
        SECTIONS[8] : 4,
        SECTIONS[9] : 4,
        SECTIONS[10] : 5,
        SECTIONS[11] : 5,
    }'''
    
    section_to_rsu = {
        SECTIONS[0] : 0,
        SECTIONS[1] : 1,
        SECTIONS[2] : 2,
        SECTIONS[3] : 3,
        SECTIONS[4] : 4,
        SECTIONS[5] : 5,
    }

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
    consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="Toll")
    #consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="Accident")
    #consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="AvgSpeed")
    consumer_switch = net.addSwitch('s%i' % (len(switches)+len(leaf_switches)))
    net.addLink(consumer_switch, consumer)
    net.addLink(consumer_switch, sub_switch, cls=TCLink, delay=delaystr, bw=baseBandwidth)
    publisherInSection = OrderedDict()
    for i in range(1, len(leaf_switches)+1):
        publisherInSection[i] = traces[i].values[0][3]
        newSwitch = intermediate_layer_switches[section_to_rsu[publisherInSection[i]]]
        pubSwitch = leaf_switches[i-1]
        net.addLink(newSwitch, pubSwitch, cls=TCLink, delay=delaystr, bw=baseBandwidth)
        #print "connections from %s to %s: %s" % (publishersdict[i], newSwitch, publishersdict[i].connectionsTo(newSwitch))
    
    for i in range(1, len(leaf_switches)+1):
        for s in intermediate_layer_switches:
            pub = leaf_switches[i-1]#publishersdict[i]
            print "connections from %s to %s: %s" % (pub, s, pub.connectionsTo(s))
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

    # simulate mobility of publishers by moving publishers between leaf switches according to tracefile position
    sched = BackgroundScheduler()
    logging.basicConfig()
    logging.getLogger('sched').setLevel(logging.INFO)
    sched.start()

    print "mobility simulated: ", mobility_enabled
    if mobility_enabled:
        
        with open('handovers.csv', "w") as file:
            file.write("")
        print "handovers.csv initialized"
        tdict = {"t": 1}
        
        def movePublisher(pubId, newSec):
            print "Moving publisher P%s to section %s" % (pubId, newSec)
            currentSection = publisherInSection[pubId]
            oldSwitch = intermediate_layer_switches[section_to_rsu[currentSection]]
            newSwitch = intermediate_layer_switches[section_to_rsu[newSec]]
            if section_to_rsu[currentSection] != section_to_rsu[newSec]:
                #_, _ = moveNodeToSwitch(net=net, node=publishersdict[pubId], oldSwitch=oldSwitch, newSwitch=newSwitch, delay=delaystr, bw=baseBandwidth)
                _, _ = moveNodeToSwitch(net=net, node=leaf_switches[pubId-1], oldSwitch=oldSwitch, newSwitch=newSwitch, delay=delaystr, bw=baseBandwidth)
            publisherInSection[pubId] = newSec
            # write link changes to file
            with open('handovers.csv', 'a') as linkChange_file:
                millis = int(round(time.time() * 1000))
                timestamp = datetime.datetime.now()
                pings = {}
                sum = 0
                # for connectivity debugging
                #for n in net.hosts:
                #    ping = n.cmd('ping -c 2 %s' % pub.IP())
                #    sent, received = net._parsePing(ping)
                #    info("ping %s -> %s %s" %(n, pub, received))
                #    pings[n] = received
                #    sum += received
                linkChange_file.write('%s,%s,%s,%s,%s,%s,%s\n' % (millis, publishersdict[pubId], oldSwitch, newSwitch, timestamp, sum, pings))
                print "Added to Handovers: %s,%s,%s,%s,%s,%s,%s" %(millis, pub, publishersdict[pubId], newSwitch, timestamp, sum, pings)

        def checkPosition():
            t = tdict["t"]
            for pub in traces:
                trace = traces[pub]
                tmp = trace[trace.Time==t].values.ravel()
                print "%s Checking position of publisher P%s in section %s with row: %s" % (t, pub, publisherInSection[pub], tmp)
                if tmp.shape[0] == 6:
                    current = publisherInSection[pub]
                    new = tmp[3]
                    if current!=new:
                        movePublisher(pubId=pub, newSec=new)
        
        def clock():
            tdict["t"] = tdict["t"] + clockInterval

        sched.add_job(checkPosition, 'interval', seconds=checkInterval, misfire_grace_time=None, coalesce=True)
        sched.add_job(clock, 'interval', seconds=clockInterval)

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
    for i in range(0, 6):
        offset = len(publishers)+i
        s = net.addSwitch(name="s%i" % offset)
        switches_l1.append(s)
        n = create_processing_node(net, "n%i" % (offset-len(publishers)+1), ip="10.0.0.%i"%(10+offset-len(publishers)), port=2520+offset)
        nodes.append(n)
        net.addLink(n,s)
    
    for i in range(0, 2):
        offset = len(publishers)+len(switches_l1)+i
        s = net.addSwitch(name="s%i" % offset)
        switches_l2.append(s)
        n = create_processing_node(net, "n%i" % (offset-len(publishers)+1), ip="10.0.0.%i" % (10 + offset-len(publishers)), port=2520+offset)
        nodes.append(n)
        net.addLink(s, switches_l1[3*i], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, switches_l1[3*i+1], cls=TCLink, delay=lat, bw=bw)
        net.addLink(s, switches_l1[3*i+2], cls=TCLink, delay=lat, bw=bw)
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
    placement = sys.argv[2] if len(sys.argv) > 2 else 'Relaxation'
    mobility = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else True
    latency = sys.argv[4] if len(sys.argv) > 4 else 10
    user = sys.argv[5] if len(sys.argv) > 5 else 'blins'
    image = str(sys.argv[6]) + '/tcep' if len(sys.argv) > 6 else 'nzumbe/tcep'
    nPublishers = int(sys.argv[7]) if len(sys.argv) > 7 else 6
    combinedPIM = int(sys.argv[8]) if len(sys.argv) > 8 else 0
    simulation(simDuration=duration, initial_algorithm=placement, mobility_enabled=mobility, baseLatency=latency,
               image=image, num_publishers=nPublishers, enable_cli=False, combinedPIM=combinedPIM)
