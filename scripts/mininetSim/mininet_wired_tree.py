#!/usr/bin/env python3

import datetime
import logging
import sys
import time
import re
from apscheduler.schedulers.background import BackgroundScheduler
from collections import OrderedDict
from mininet.node import Controller
from mininet.link import TCLink
from mininet.log import setLogLevel, info, debug
from mininet.net import Containernet
from mobility import MobilitySwitch, moveHost
from mininet.cli import CLI

import traceLoader as tl

simMode = 13 # 9 - general thesis | 5 - SPLC/CONTRAST / 10 - Accident


# create a CEP processing node and add it to the mininet topology
def create_processing_node(topology, name, ip, port):
    jmx_port = int(port) + 1000
    n = topology.addDocker(
        name, dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.machinenodes.EmptyApp",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--ip " + str(ip) + " --port " + str(port), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["%s/logs/%s:/app/logs" % (homedir, name)],
        ports=[port, jmx_port],
        port_bindings={port: port, jmx_port:jmx_port})
    n.cmd('nohup ./docker-entrypoint.sh > nohup.out &')
    return n

def create_consumer(topology, name, ip, port, kind):
    jmx_port = int(port) + 1000
    n = topology.addDocker(
        name, dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.machinenodes.ConsumerApp",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--ip " + str(ip) + " --port " + str(port) + " --kind " + str(kind), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["%s/logs/%s:/app/logs" % (homedir, name)],
        ports=[port, jmx_port],
        port_bindings={port: port, jmx_port:jmx_port}
    )
    n.cmd("nohup ./docker-entrypoint.sh > nohup.out &")
    return n


# create a CEP publisher and add it to the mininet topology
# kind: SpeedPublisher (others can be defined in PublisherApp.scala)
# p1 = create_publisher(topology=net, name='p1', ip='10.0.0.101', port='publisher_base_port', kind='SpeedPublisher', id=1)
def create_publisher(topology, name, ip, port, kind, id):
    jmx_port = int(port) + 1000
    p = topology.addDocker(
        name, dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.machinenodes.PublisherApp",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--ip " + str(ip) + " --port " + str(port) + " --kind " + str(
                         kind) + " --numberOfPublishers " + str(id), "JMX_PORT=%d" % jmx_port],
        ip=ip,
        volumes=["%s/logs/%s:/app/logs" % (homedir, name),
                 "%s/tcep/mobility_traces:/app/mobility_traces" % homedir],
        ports=[port, jmx_port], port_bindings={port: port, jmx_port:jmx_port})
    p.cmd('nohup ./docker-entrypoint.sh > nohup.out &')
    return p

# simulate node mobility by removing the link to the old switch and creating a new link to the new switch
def moveNodeToSwitch(net, node, oldSwitch, newSwitch, delay, bw):
    info("*** Moving %s from %s to %s \n" % (node, oldSwitch, newSwitch))
    debug("connections from %s to %s: %s \n" % (node, oldSwitch, node.connectionsTo(oldSwitch)))
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
    setupDelay = 5  # s; time to wait before starting the simulation in seconds
    clockInterval = 0.5  # s
    checkInterval = 10.0  # s; update interval for checking if a publisher needs to be moved to the next switch
    stopAtHalf = False  # insert a 5-minute period where publisher mobility is disabled after half the simulation / at the start
    stopAtStart = True  # !!!! DO NOT start moving publishers around before akka cluster is up !!!
    # dictionary with keys 1 to sections * vehiclesPerSection,
    # values trace data frame (time, position, speed) for each vehicle
    traces, rsu_range, num_rsus, road_length = tl.load_traces(num_publishers, publishers_per_section,
                                                              trace_file='mobility_traces/A6-d11-h08.dat', stop_at_start=stopAtStart, stop_at_half=stopAtHalf)
    section_length = rsu_range * 2
    num_sections = int(num_publishers / publishers_per_section)
    # not tested, might work
    # net = Containernet( switch=MobilitySwitch )
    # floodlight = net.addController(name='floodlight', controller=RemoteController, ip='127.0.0.1', port=6653)

    net = Containernet(controller=Controller, switch=MobilitySwitch)
    info('*** Adding controller\n')
    c0 = net.addController('c0')

    info('*** Adding docker containers\n')
    publishersdict = {}
    leaf_switches = []
    nodes = []
    for i in range(0, num_sections):
        s = net.addSwitch(name='s%i' % len(net.switches))
        leaf_switches.append(s)
        for pi in range(0, nPublishersPerSection):
            offset = i * nPublishersPerSection + pi
            p = create_publisher(topology=net, name='sp%i' % (offset + 1), ip='10.0.0.%i' % (101 + offset), port='%i' % (publisher_base_port + offset),
                                 kind='SpeedPublisher', id=(offset + 1))
            publishersdict[offset + 1] = p
            net.addLink(p, s) # TCLink has no effect between switch and host
            nodes.append(p)

    # creates the necessary switches and processing nodes for the given number of publishers; each switch has two children (or three if uneven number)
    switches, upper_layer_switches, intermediate_layer_switches, nodes, max_hops = create_tree_topology(net, leaf_switches,
                                                                                          nodes, baseLatency, baseBandwidth)

    sub = net.addDocker(
        name='simulator', dimage=image,
        environment=["HOST_UID="+str(user), "MAIN=tcep.simulation.tcep.SimulationRunner",
                     "LOG_FILE_PATH=/app/logs",
                     "ARGS=--dir ./logs --mode " + str(simMode) + " --ip 10.0.0.253 --port " + str(base_port) + " --duration " + str(simDuration) + \
                     " --initialAlgorithm " + str(initial_algorithm) +
                     " --baseLatency " + str(baseLatency) +
                     " --numberOfPublishers " + str(num_publishers) +
                     " --maxPubToClientHops " + str(max_hops) + 
                     " --combinedPIM " + str(combinedPIM), "JMX_PORT=8484"],
        ip='10.0.0.253',
        volumes=["%s/logs/simulation:/app/logs" % homedir,
                 "%s/tcep/handovers.csv:/app/handovers.csv" % homedir],
        ports=[base_port, 8484, 5266, 25001], port_bindings={base_port: base_port, 8484:8484, 5266:5266, 25001:25001})
    sub.cmd('nohup ./docker-entrypoint.sh > nohup.out &')
    nodes.append(sub)
    info("switches: %i %i" % (len(switches), len(leaf_switches)))
    info(net.switches)
    sub_switch = net.addSwitch(name='s%i' % len(net.switches))
    switches.append(sub_switch)
    net.addLink(sub_switch, sub)
    gui = net.addDocker(name='gui', dimage=image.split("/")[0]+"/tcep-gui:latest", ip='10.0.0.254', volumes=["%s/logs/gui:/usr/src/app/log" % homedir], ports=[3000],
                        port_bindings={3000:3000})
    gui.cmd('npm start > log/gui_log.txt &')
    net.addLink(sub_switch, gui)
    for s in upper_layer_switches:
        net.addLink(sub_switch, s, cls=TCLink, delay=delaystr, bw=baseBandwidth)
    consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="Accident")
    nodes.append(consumer)
    #consumer = create_consumer(topology=net, name="Subscriber", ip="10.0.0.50", port="2700", kind="AvgSpeed")
    consumer_switch = net.addSwitch('s%i' % (len(net.switches)))
    switches.append(consumer_switch)
    net.addLink(consumer_switch, consumer)
    net.addLink(consumer_switch, sub_switch, cls=TCLink, delay=delaystr, bw=baseBandwidth)

    # add single density publisher to network, publishes for all sections
    s = net.addSwitch(name='s%i' % len(net.switches))
    info("\n adding switch for density publisher: %s\n" % s)
    switches.append(s)
    dp = create_publisher(topology=net, name='dp', ip='10.0.0.%i' % (101 + nPublishers), port='%i' % (publisher_base_port + nPublishers), kind='DensityPublisher', id=0)
    nodes.append(dp)
    net.addLink(s, dp)
    net.addLink(sub_switch, s, cls=TCLink, delay='%sms' % str(int(baseLatency) * 3), bw=baseBandwidth)

    ##########################################################
    # simulated network topology 12 publishers in 4 sections #
    ##########################################################
    #              client
    #                n0
    #           n1         n2
    #        n3   n4    n5    n6
    # p    1 2 3|4 5 6|7 8 9|10 11 12
    #      -->-->-->-->-->-->-->-->--> movement direction

    ###############################################
    # SIMULATION START                            #
    ###############################################
    info('*** Starting network\n')
    net.start()

    start_time = time.time()
    time_passed = time.time() - start_time + 1
    if (time_passed < setupDelay):
        print("waiting ", setupDelay - time_passed, "s before starting ...")
        time.sleep(setupDelay - time_passed)

    info('\n*** starting simulation with %i nodes, %i switches and %i links \n****\n' % (len(net.hosts), len(net.switches), len(net.links)))

    # simulate mobility of publishers by moving publishers between leaf switches according to tracefile position
    sched = BackgroundScheduler()
    logging.basicConfig()
    logging.getLogger('sched').setLevel(logging.INFO)
    sched.start()

    print("mobility simulated: ", mobility_enabled)
    if mobility_enabled:
        # dictionary with publisher number -> current section index (0 to numSections - 1)
        publishersInSection = OrderedDict()
        for i in range(1, nPublishers + 1):
            publishersInSection[i] = int((i - 1) / publishers_per_section)
        print("publisher->section dict: %s\n" % publishersInSection)

        # for i in range(1, nPublishers + 1):
        #    info("traces publisher %s" % i)
        #    info(traces[i][58:62])

        with open('handovers.csv', 'w') as file:
            file.write("")

        tdict = {"t": 0.0}

        # check if a vehicle has moved out of the range of the switch it is currently connected to
        def isNotInSectionRange(pos, sec):
            position = float(pos)
            start = sec * section_length
            end = sec * section_length + section_length
            if (position < start) | (position > end):  # | (((sec+1) == numSections) & (pos > (end-20)))
                return True
            else:
                return False

        # move a publisher to the next switch
        def movePublisherForward(pub, pubid):
            currsec = publishersInSection[pubid]
            newsec = publishersInSection[pubid] + 1
            if currsec == num_sections - 1:
                newsec = 0
            oldswitch = leaf_switches[currsec]
            newswitch = leaf_switches[newsec]
            debug("leaf_switches: %s" % leaf_switches)
            info("moving publisher %s forward from %s to %s" % (pub, oldswitch, newswitch))
            nodeIntf, newswitchIntf = moveNodeToSwitch(net=net, node=pub, oldSwitch=oldswitch, newSwitch=newswitch,
                                                       delay=delaystr, bw=baseBandwidth)
            publishersInSection[pubid] = newsec
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
                linkChange_file.write('%s,%s,%s,%s,%s,%s,%s\n' % (millis, pub, oldswitch, newswitch, timestamp, sum, pings))

        def checkPosition():
            # print "*** \nposition checks: "
            for pubId in range(1, len(publishersdict) + 1):
                time = tdict["t"]
                position = traces[pubId].loc[time]["position"]
                currsec = publishersInSection[pubId]
                # print "*** : p " + str(pubId) , currsec, position
                moveForward = isNotInSectionRange(pos=position, sec=currsec)

                if moveForward:
                    pub = publishersdict[pubId]
                    movePublisherForward(pub=pub, pubid=pubId)

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


def create_tree_topology(net, section_switches, nodes, lat, bw):
    # connect the given list of publishers' switches
    # create (2-3) layered tree hierarchy with publishers/cars as leaves and processing nodes at nodes
    # each node has 2-3 children
    switches_l1 = []  # road-side units / switches connecting to publishers' switches (additional switch needed for TCLink to work)
    switches_l2 = []  # layer connecting road-side switches
    info('*** creating wired tree topology with %i publishers distributed to %i sections (%i per section) \n' % (nPublishers, nPublishers / nPublishersPerSection, nPublishersPerSection))
    assert(len(section_switches) * nPublishersPerSection == nPublishers)

    for i in range(0, len(section_switches)):
        s = net.addSwitch(name='s%i' % len(net.switches))
        net.addLink(s, section_switches[i], cls=TCLink, delay=lat, bw=bw)
        switches_l1.append(s)
        offset = len(nodes) - nPublishers + 1
        n = create_processing_node(net, 'n%i' % offset, ip='10.0.0.%i' % (10 + offset), port=(base_port - offset))
        nodes.append(n)
        net.addLink(n, s)

    # add an additional layer if necessary
    if len(switches_l1) > 3:
        for i in range(0, len(switches_l1) - (len(switches_l1) % 2), 2):  # treat odd number of switches by adding last after loop
            s = net.addSwitch(name='s%i' % len(net.switches))
            switches_l2.append(s)
            offset = len(nodes) - nPublishers + 1
            n = create_processing_node(net, 'n%i' % offset, ip='10.0.0.%i' % (10 + offset), port=(base_port - offset))
            nodes.append(n)
            net.addLink(s, switches_l1[i], cls=TCLink, delay=lat, bw=bw)
            net.addLink(s, switches_l1[i + 1], cls=TCLink, delay=lat, bw=bw)
            net.addLink(n, s)

        if len(switches_l1) % 2 == 1:
            net.addLink(switches_l1[-1], switches_l2[-1], cls=TCLink, delay=lat, bw=bw)

    # car -> l1 -> l2 (?) -> sub
    max_hops = 3 if switches_l2 else 2
    return switches_l1 + switches_l2, switches_l2 if switches_l2 else switches_l1, switches_l1, nodes, max_hops


if __name__ == '__main__':
    setLogLevel('debug')

    # global constants
    with open("application.conf", encoding='utf-8') as f:
        base_port = int(re.compile(r'base-port = \d+').search(f.read()).group(0).split(' = ')[1])

    publisher_base_port = base_port + 1
    # duration = simulation duration in minutes
    # initial_algorithm = CEP placement algorithm to start with
    # mobility_enabled = enable or disable publisher mobility simulation
    # link_latency = base link latency between switches
    duration = sys.argv[1] if len(sys.argv) > 1 else 10
    placement = sys.argv[2] if len(sys.argv) > 2 else 'MDCEP'
    mobility = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else True
    latency = sys.argv[4] if len(sys.argv) > 4 else 10
    homedir = str(sys.argv[5]) if len(sys.argv) > 5 else '/home/niels'
    user = homedir.split('/')[2]
    image = str(sys.argv[6]) + '/tcep' if len(sys.argv) > 6 else 'nieda2018/tcep'
    nPublishers = int(sys.argv[7]) if len(sys.argv) > 7 else 8
    nPublishersPerSection = int(sys.argv[8]) if len(sys.argv) > 8 else 2
    combinedPIM = int(sys.argv[9]) if len(sys.argv) > 9 else 0
    simulation(simDuration=duration, initial_algorithm=placement, mobility_enabled=mobility, baseLatency=latency,
               image=image, num_publishers=nPublishers, publishers_per_section=nPublishersPerSection, enable_cli=False,
               combinedPIM=combinedPIM)
