import kafka
import datetime
import json
import io
import fcntl
import time
import string
import os

import RPi.GPIO as GPIO
import sys
import thread
import tty
import termios
breakNow = False

def getch():

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(sys.stdin.fileno())
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch

def waitForKeyPress():
    global breakNow
    while True:
        ch = getch()
        if ch == "b": # Or skip this check and just break
            breakNow = True
            break

#Producer = kafka.KafkaProducer(bootstrap_servers='137.135.125.3:9092')
#Topic = 'test'
Controller_ID = 'C001'
class AtlasI2C:
    # the timeout needed to query readings and calibrations
    long_timeout = .8
    short_timeout = .8
    current_addr = None

    def __init__(self, address, bus):
        # open two file streams, one for reading and one for writing
        # the specific I2C channel is selected with bus
        # it is usually 1, except for older revisions where its 0
        # wb and rb indicate binary read and write
        self.file_read = io.open("/dev/i2c-" + str(bus), "rb", buffering=0)
        self.file_write = io.open("/dev/i2c-" + str(bus), "wb", buffering=0)
        # initializes I2C to either a user specified or default address
        self.set_i2c_address(address)

    def set_i2c_address(self, addr):
        '''Set the I2C communications to the slave specified by the address.
        The commands for I2C dev using the ioctl functions are specified in
        the i2c-dev.h file from i2c-tools'''
        I2C_SLAVE = 0x703
        fcntl.ioctl(self.file_read, I2C_SLAVE, addr)
        fcntl.ioctl(self.file_write, I2C_SLAVE, addr)
        self.current_addr = addr

    def write(self, cmd):
        '''Appends the null character and sends the string over I2C'''
        cmd += "\00"
        self.file_write.write(cmd)

    def read(self, num_of_bytes=31):
        '''Reads a specified number of bytes from I2C, then parses and displays the result'''
        res = self.file_read.read(num_of_bytes)  # read from the board
        response = filter(lambda x: x != '\x00', res)  # remove the null characters to get the response
        if ord(response[0]) == 1:  # if the response isn't an error
            # change MSB to 0 for all received characters except the first and get a list of characters
            char_list = map(lambda x: chr(ord(x) & ~0x80), list(response[1:]))
            # NOTE: having to change the MSB to 0 is a glitch in the raspberry pi, and you shouldn't have to do this!
            return ''.join(char_list)  # convert the char list to a string and returns it
        else:
            return "Error " + str(ord(response[0]))

    def query(self, string):
        '''Write a command to the board, wait the correct timeout, and read the response'''
        self.write(string)
        # The read and calibration commands require a longer timeout
        if (string.upper().startswith("R")):
            time.sleep(self.long_timeout)
        elif (string.upper().startswith("CAL")):
            time.sleep(self.short_timeout)
        else:
            time.sleep(self.short_timeout)
        return self.read()

    def close(self):
        self.file_read.close()
        self.file_write.close()

def identifyDevices():
    i2c = os.popen("sudo i2cdetect -y 1").read().split()
    addr = []
    for i in range(len(i2c)):
        if ((i2c[i][1:3] != "0:") & (i2c[i][1:2] != "")):
            if (i2c[i] != "--"):
                addr.append(int(i2c[i],16))
    return addr

def printDevOptions(devAddr):
    count = 1
    for a in devAddr:
        rec = False
        sensor = AtlasI2C(address=a, bus=1)
        while not rec:
            try:
                rec = True
                value = sensor.query("i")
                data = value.split(',')
                devName = data[1]
                devFirmware = data[2]
            except:
                rec = False
        print str(count) + '. Name: ' + devName + ', Firmware: ' + devFirmware
        count += 1
        # time.sleep(1)
    print str(count) + '. All'
    count += 1
    print str(count) + '. Exit'
    option = input('Choose a device from above list to read data from by entering its serial number: ')
    valid = False
    while not valid:
        if option < 1 or option > count:
            option = input('Please choose a valid option: ')
        else:
            valid = True
    return option

def getAtlasI2cData(addr):
    sensor = AtlasI2C(address= addr, bus=1)
    value = sensor.query("i")
    print value
    time.sleep(1)
    current_time = datetime.datetime.now().isoformat()
    value = sensor.query("R")
    data = {'current_time': current_time, 'sensor_value': value, 'ID': Controller_ID}
    payload = json.dumps(data)
    print data
    #Producer.send(Topic, payload)
    time.sleep(5)

def getData(devAddr,option):
    if option > len(devAddr):
        while breakNow == False:
            for a in devAddr:
                getAtlasI2cData(a)
    else:
        while breakNow == False:
            getAtlasI2cData(devAddr[option-1])



def main():
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(17, GPIO.OUT,initial=GPIO.LOW)
    GPIO.setup(27, GPIO.OUT,initial=GPIO.LOW)
    while(True):
        GPIO.output(17, 1)
        print 'sensor 1 start'
        time.sleep(15)
        GPIO.output(17, GPIO.LOW)
        print 'sensor 1 stop'
        time.sleep(15)
        GPIO.output(27, 1)
        print 'sensor 2 start'
        time.sleep(15)
        GPIO.output(27, GPIO.LOW)
        print 'sensor 2 stop'
        time.sleep(15)
        print 'sensors ended'
    # devAddr = identifyDevices()
    # print str(len(devAddr))+' devices identified'
    # option = printDevOptions(devAddr)
    # if option == len(devAddr)+2:
    #     print "Exited!"
    # else:
    #     thread.start_new_thread(waitForKeyPress,())
    #     getData(devAddr, option)

    #Producer.send(Topic,)

main()
