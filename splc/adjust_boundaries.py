import numpy as np
import pandas as pd
import sys
import xml.dom.minidom

def adjust_boundaries():

    def get_value(node, tag):
        return node.getElementsByTagName(tag)[0].childNodes[0].data

    def update_value(node, tag, new_value):
        node.getElementsByTagName(tag)[0].childNodes[0].data = new_value

    #
    data = pd.read_csv('~/splc/combined_measurements.csv', sep=';', header=0)
    # use the parse() function to load and parse an XML file
    doc = xml.dom.minidom.parse("cfm.xml")
    vm = doc.documentElement
    binaryOptions = vm.getElementsByTagName("binaryOptions")[0].getElementsByTagName("configurationOption")
    numericOptions = vm.getElementsByTagName("numericOptions")[0].getElementsByTagName("configurationOption")

    for index, row in data.iterrows():

        for binConfigOpt in binaryOptions:
            name = get_value(binConfigOpt, "name")
            #print "name %s" % name

        for numOpt in numericOptions:
            feature = get_value(numOpt, "name")
            value = float(row[feature])
            lb = float(get_value(numOpt, "minValue"))
            ub = float(get_value(numOpt, "maxValue"))
            step = get_value(numOpt, "stepFunction")
            if (value < lb) or (value > ub):
                print "invalid NumericOption " + str(feature) + " in row " + str(index)
                print str(value) + " not in (" + str(lb) + ", " + str(ub) + ")"
                print "adjusting boundaries"

            if value < lb:
               update_value(numOpt, "minValue", value)

            if value > ub:
                update_value(numOpt, "maxValue", value)

    # write adjusted attribute boundaries
    f = open("cfm.xml", 'w')
    doc.writexml( f )
    f.close()


adjust_boundaries()