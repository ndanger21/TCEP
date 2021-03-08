#!/usr/bin/python3

"""
Simple example of Mobility with Mininet
(aka enough rope to hang yourself.)

We move a host from s1 to s2, s2 to s3, and then back to s1.

Gotchas:

The reference controller doesn't support mobility, so we need to
manually flush the switch flow tables!

Good luck!

to-do:

- think about wifi/hub behavior
- think about clearing last hop - why doesn't that work?
"""

from mininet.cli import CLI
from mininet.node import OVSSwitch, Controller
from mininet.link import TCLink
from mininet.log import output, warn
from mininet.net import Containernet


class MobilitySwitch( OVSSwitch ):
    "Switch that can reattach and rename interfaces"

    def delIntf( self, intf ):
        "Remove (and detach) an interface"
        port = self.ports[ intf ]
        del self.ports[ intf ]
        del self.intfs[ port ]
        del self.nameToIntf[ intf.name ]

    def addIntf( self, intf, rename=False, **kwargs ):
        "Add (and reparent) an interface"
        OVSSwitch.addIntf( self, intf, **kwargs )
        intf.node = self
        if rename:
            self.renameIntf( intf )

    def attach( self, intf ):
        "Attach an interface and set its port"
        port = self.ports[ intf ]
        if port:
            if self.isOldOVS():
                self.cmd( 'ovs-vsctl add-port', self, intf )
            else:
                self.cmd( 'ovs-vsctl add-port', self, intf,
                          '-- set Interface', intf,
                          'ofport_request=%s' % port )
            #self.validatePort( intf )

    def validatePort( self, intf ):
        "Validate intf's OF port number"
        ofport = int( self.cmd( 'ovs-vsctl get Interface', intf,
                                'ofport' ) )
        if ofport != self.ports[ intf ]:
            warn( 'WARNING: ofport for', intf, 'is actually', ofport,
                  '\n' )

    def renameIntf( self, intf, newname='' ):
        "Rename an interface (to its canonical name)"
        intf.ifconfig( 'down' )
        if not newname:
            newname = '%s-eth%d' % ( self.name, self.ports[ intf ] )
        intf.cmd( 'ip link set', intf, 'name', newname )
        del self.nameToIntf[ intf.name ]
        intf.name = newname
        self.nameToIntf[ intf.name ] = intf
        intf.ifconfig( 'up' )

    def moveIntf( self, intf, switch, port=None, rename=True ):
        "Move one of our interfaces to another switch"
        self.detach( intf )
        self.delIntf( intf )
        switch.addIntf( intf, port=port, rename=rename )
        switch.attach( intf )


def printConnections( switches ):
    "Compactly print connected nodes to each switch"
    for sw in switches:
        output( '%s: ' % sw )
        for intf in sw.intfList():
            link = intf.link
            if link:
                intf1, intf2 = link.intf1, link.intf2
                remote = intf1 if intf1.node != sw else intf2
                output( '%s(%s) ' % ( remote.node, sw.ports[ intf ] ) )
        output( '\n' )


def moveHost( host, oldSwitch, newSwitch, newPort=None ):
    "Move a host from old switch to new switch"
    hintf, osintf = host.connectionsTo( oldSwitch )[ 0 ]
    oldSwitch.moveIntf( osintf, newSwitch, port=newPort )
    hintf, nsintf = host.connectionsTo( newSwitch )[ 0 ]
    return hintf, nsintf


def mobilityTest():
    "A simple test of mobility"
    print('* Simple mobility test')
    net = Containernet(controller=Controller, switch=MobilitySwitch)
    #net = Mininet( controller=Controller, switch=MobilitySwitch )
    c0 = net.addController('c0')
    h1 = net.addDocker('h1', ip='10.0.0.254', dimage="ubuntu:trusty", ports=[2500], port_bindings={2500:2500})
    h2 = net.addDocker('h2', ip='10.0.0.253', dimage="ubuntu:trusty")
    h3 = net.addDocker('h3', ip='10.0.0.252', dimage="ubuntu:trusty")
    #info('*** Connecting Docker Hosts to Switches')

    print('*** Adding switches\n')
    s1 = net.addSwitch('s1')
    s2 = net.addSwitch('s2')
    s3 = net.addSwitch('s3')

    print('*** Creating links\n')
    net.addLink(h1, s1, cls= TCLink, delay='10ms', bw=100)
    net.addLink(h2, s2, cls= TCLink, delay='10ms', bw=100)
    net.addLink(h3, s3, cls= TCLink, delay='10ms', bw=100)
    net.addLink(s1, s2, cls=TCLink, delay='10ms', bw=100)
    net.addLink(s2, s3, cls=TCLink, delay='10ms', bw=100)

    print('* Starting network:')
    net.start()
    printConnections( net.switches )
    print('* Testing network')
    #net.pingAll() # IMPORTANT! initialization
    net.pingAllFull()
    print('* Identifying switch interface for h1')
    h1, old = net.get( 'h1', 's1' )
    for s in 2, 3:
        new = net[ 's%d' % s ]
        print('* Moving', h1, 'from', old, 'to', new)
        hintf, sintf = moveHost( h1, old, new )
        print('*', hintf, 'is now connected to', sintf)
        print('* Clearing out old flows')
        for sw in net.switches:
            sw.dpctl( 'del-flows' )
        # modified
        # adds 50ms IN ADDITION to existing delay on this interface
        hintf.config(bw = 100, delay='50ms')
        sintf.config(bw = 100, delay='50ms')
        print( '\n*** after adding delay:\n' )
        net.pingAll()
        net.pingAllFull()
        print('* New network:')
        printConnections( net.switches )
        print('* Testing connectivity:')
        net.pingAll()
        old = new
    CLI(net)
    net.stop()

if __name__ == '__main__':
    mobilityTest()
