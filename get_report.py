#!/usr/bin/python
#
from __future__ import print_function
from metar import Metar

import os
import sys
import getopt
import string
import math
import json
import numpy as np
from metpy.units import units
import metpy.calc as mpcalc
import paho.mqtt.client as mqtt

try:
    from urllib2 import urlopen
except:
    from urllib.request import urlopen

BASE_URL = "http://tgftp.nws.noaa.gov/data/observations/metar/stations"
MQTT_PUB_ROOT = "METAR"
broker_address = "mqtt.int.nforcer.com"
stations = ["KMWO"]


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def truncate(f, n):
    return math.floor(f * 10 ** n) / 10 ** n


def mqtt_publish(station, dewpoint, temperature, rh, pressure):
    print("Station: ", station, " Dew point: ", dewpoint, " Temp: ",
          temperature, " RH%: ", rh, " Pressure: ", pressure, "\n")
    dict = {"station": station, "dewpoint": dewpoint,
            "temp": temperature, "humidity": rh, "pressure": pressure}
    client = mqtt.Client("metar")  # create new instance
    client.connect(broker_address)  # connect to broker
    client.publish(MQTT_PUB_ROOT, payload=json.dumps(dict))  # publish


for name in stations:
    url = "%s/%s.TXT" % (BASE_URL, name)
    try:
        urlh = urlopen(url)
        report = ""
        for line in urlh:
            if not isinstance(line, str):
                line = line.decode()  # convert Python3 bytes buffer to string
            if line.startswith(name):
                report = line.strip()
                obs = Metar.Metar(line)
                temp = obs.temp._value * units.degC
                dewp = obs.dewpt._value * units.degC
                hum = truncate(
                    (mpcalc.relative_humidity_from_dewpoint(temp, dewp)).m * 100, 2)  # convert to %
                pressure = truncate(obs.press._value*33.864, 2)
                mqtt_publish(obs.station_id, obs.dewpt._value,
                             obs.temp._value, hum, pressure)
                break
        if not report:
            print("No data for ", name, "\n\n")
    except Metar.ParserError as exc:
        print("METAR code: ", line)
        print(string.join(exc.args, ", "), "\n")
    except:
        import traceback

        print(traceback.format_exc())
        print("Error retrieving", name, "data", "\n")
