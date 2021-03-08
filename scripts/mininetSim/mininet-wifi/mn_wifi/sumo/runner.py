import sys
import os
import time
import threading
import datetime
from threading import Thread as thread


from mininet.log import info, debug
from mn_wifi.mobility import Mobility
from mn_wifi.sumo.sumolib.sumolib import checkBinary
from mn_wifi.sumo.traci import _vehicle, _poi
from mn_wifi.sumo.traci import main as trace


class sumo(Mobility):

    vehCmds = None

    def __init__(self, cars, aps, **kwargs):
        self.ac = 'ssf'
        Mobility.thread_ = thread(name='vanet', target=self.configureApp,
                                  args=(cars, aps), kwargs=dict(kwargs,))
        Mobility.thread_.daemon = True
        Mobility.thread_._keep_alive = True
        Mobility.thread_.start()

    @classmethod
    def getVehCmd(cls):
        return cls.vehCmds

    def configureApp(self, cars, aps, stations=[], config_file='map.sumocfg', sumo_gui=True,
                     clients=1, port=8813, delay=1.0):
        try:
            Mobility.cars = cars
            Mobility.aps = aps
            Mobility.mobileNodes = cars
            debug("*** assigned cars, aps and mobile nodes to Mobility")
            self.start(cars, aps, stations, config_file, clients, port, delay, sumo_gui)
        except:
            e = sys.exc_info()[0]
            info("Error: %s\n" % e, "*** Connection with SUMO has been closed\n")


    def setWifiParameters(self):
        thread = threading.Thread(name='wifiParameters', target=self.parameters)
        thread.start()

    def start(self, cars, aps, stations, config_file, clients, port, delay, sumo_gui):
        sumoBinary = checkBinary('sumo-gui') if sumo_gui else checkBinary('sumo')
        info("*** found SUMO binary: %s\n" % sumoBinary)
        sumoConfig = os.path.join(os.path.dirname(__file__), "data/%s" % config_file)
        info("*** fround SUMO configfile %s\n" % sumoConfig)
        info("*** calling sumo with args: %s -c %s --start --num-clients %s --remote-port %s --time-to-teleport -1"
              % (sumoBinary, sumoConfig, clients, port))
        os.system(' %s -c %s --start --num-clients %s '
                  '--remote-port %s --time-to-teleport -1 &'
                  % (sumoBinary, sumoConfig, clients, port))

        trace.init(port)
        trace.setOrder(0)
        # get the positions of rsus and their stations (immobile)
        poiCmds = _poi.PoiDomain()
        poiCmds._connection = trace.getConnection(label='default')
        poiIDs = poiCmds.getIDList()
        info("*** retrieved AP->poi list %s " % ', '.join(poiIDs))
        for i in range(0, len(poiIDs)):
            pos = poiCmds.getPosition(poiIDs[i])
            if i < len(aps):
                try:
                    aps[i].position = int(pos[0]), int(pos[1]), 0
                    # small offset so stations actually associate
                    if len(stations) > i:
                        stations[i].position = int(pos[0])+2, int(pos[1])+2, 0
                    info("*** set position of AP %s : %s\n" % (poiIDs[i], aps[i].position))
                    aps[i].position = aps[i].position
                    info("*** setting highlight for wifi range on AP %i" % i )
                    poiCmds.highlight(poiIDs[i], size=aps[i].wintfs[0].range, color=(0, 255, 255, 255))
                    if len(stations) > i:
                        stations[i].position = stations[i].position

                except:
                    e = sys.exc_info()
                    info("Error: %s %s\n %s\n" % (e[0], e[1], e[2]), i)


        step = 0
        debug("*** setting wifi parameters for sumo mobility")
        self.setWifiParameters()
        debug("*** getting vehicle domain")
        vehCmds = _vehicle.VehicleDomain()
        debug("*** getting traci vehicle connection")
        vehCmds._connection = trace.getConnection(label="default")

        car_to_vehID = {}
        initial_pause_done = False
        while True:
            try:
                trace.simulationStep()
                curr_vehIDs = vehCmds.getIDList()
                if not initial_pause_done:
                    info("*** making trace simulation step %i before initial pause (pause: %s), car -> vehicle id mapping: %s (cars: %s, total vehicles: %s)\n" % (step, initial_pause_done, str(car_to_vehID), len(cars), len(curr_vehIDs)))

                debug('all %s current vehicle ids: %s\n' % (len(curr_vehIDs), curr_vehIDs))
                debug('mapping: %s\n' % car_to_vehID)
                for i in range(0, len(cars)):
                    # enough vehicles in sumo for all cars
                    if len(cars) <= len(curr_vehIDs):
                        # car had an id assigned before
                        vehID = car_to_vehID.get(i)
                        if vehID is not None:
                            # check if id is still active or has left the simulation
                            if vehID not in curr_vehIDs:
                                info('vehicle %s assigned to car %s has left, looking for replacement... -> ' % (vehID, car))
                                for v in curr_vehIDs:
                                    # chose new unused id if it has left simulation
                                    if v not in car_to_vehID.values():
                                        info('found replacement %s\n' % v)
                                        vehID = v
                                        car_to_vehID[i] = v
                                        vehCmds.setColor(vehID, (255, 0, 0))
                                        break
                        # car without an id assigned
                        else:
                            for v in curr_vehIDs:
                                # chose new unused id
                                if v not in car_to_vehID.values():
                                    vehID = v
                                    car_to_vehID[i] = v
                                    vehCmds.setColor(vehID, (255, 0, 0))
                                    break
                        # skip this car if no unused id could be found
                        if vehID is None:
                            continue
                        # update position of car with position of vehID
                        pos = vehCmds.getPosition(vehID)
                        cars[i].position = pos[0], pos[1], 0

                        if hasattr(cars[i], 'sumo'):
                            if cars[i].sumo:
                                args = [cars[i].sumoargs]
                                cars[i].sumo(vehID, vehCmds, *args)
                                del cars[i].sumo
                step += 1
                # after assigning vehicle positions for the first time, pause mobility for a while so that akka cluster can start without problems
                if (len(cars) <= len(curr_vehIDs)) & (not initial_pause_done):
                    info('*** %s: %i vehicles have entered the sumo simulation, enough for %i mininet cars, '
                         'halting mobility simulation for 45s so akka cluster can start...\n' % (datetime.datetime.now(), len(curr_vehIDs), len(cars)))
                    time.sleep(45)
                    info('*** %s resuming mobility simulation\n' % datetime.datetime.now())
                    initial_pause_done = True
                    for car in cars:
                        info('%s -> %s  ' % (car.name, car.wintfs[0].associatedTo))

                # slow down to 1 step / second after there is at least one sumo vehicle for each mininet car
                if initial_pause_done:
                    time.sleep(delay)
                else:
                    time.sleep(0.01)
            except:
                e = sys.exc_info()
                info("Error: %s %s\n %s\n" % (e[0], e[1], e[2]))
                break

        trace.close()
        sys.stdout.flush()
