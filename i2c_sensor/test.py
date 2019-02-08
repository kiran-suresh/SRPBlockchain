import kafka
import datetime
import json
import io
import fcntl
import time
import string

#Producer = kafka.KafkaProducer(bootstrap_servers='137.135.125.3:9092')
Topic = 'test'
Controller_ID = 'C001'
class AtlasI2C:
    # the timeout needed to query readings and calibrations
    long_timeout = 1.1
    short_timeout = 1.1
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
        #response = ""
        #for i in res:
        #    #response += 
        #    print ord(i)
        print res
        response = res.split('\x00')[0]
        print response
        
        #response = filter(lambda x: x != '\x00', res) # remove the null characters to get the response
        #response = filter(lambda x: x != '\xff', res)
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

sensor = AtlasI2C(address=105, bus=1)
i=0
while True:
	current_time = datetime.datetime.now().isoformat()
	value = sensor.query("R")
	i = i+1
	data = {'count' : i, 'current_time': current_time, 'sensor_value': value, 'ID': Controller_ID}
	payload = json.dumps(data)
	print payload
	#Producer.send(Topic, payload)
	time.sleep(2)

#Producer.send(Topic,)
