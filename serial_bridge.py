import serial
import json
from cassandra.cluster import Cluster
import datetime
import sys

cluster = Cluster(['192.168.1.3'])
session = cluster.connect('home')

ser = serial.Serial('/dev/ttyUSB0', 9600,
    timeout=None, xonxoff=False, rtscts=False, dsrdtr=False)
ser.flushInput()
ser.flushOutput()

while True:
    line = ser.readline()
    if line:
        in_data = line
        print in_data
        try:
            json_data = json.loads(in_data)
            if json_data["source"] and json_data["source"].startswith("G"):
                today = datetime.date.today()
                now = datetime.datetime.now()
                session.execute("""
                    INSERT INTO greenhouse (
                        source, day, time, temperaturein,
                        temperatureout, temperaturecheck,
                        humidity, light)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        json_data["source"],
                        today,
                        now,
                        json_data["temperaturein"],
                        json_data["temperatureout"],
                        json_data["temperaturecheck"],
                        json_data["humidity"],
                        json_data["light"])
                )
        except ValueError:
            print "ignoring value"
        except:
            print datetime.datetime.now()
            print "Unexpected error:", sys.exc_info()[0]
