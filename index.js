const util = require('util');
const Mutex = require('async-mutex').Mutex;
const mqtt = require('mqtt');
const ModbusRTU = require("modbus-serial");
const Parser = require('binary-parser').Parser;
const commandLineArgs = require('command-line-args')
const dgram = require('node:dgram');
const server = dgram.createSocket('udp4');
const { Buffer } = require('node:buffer');
const rx = Buffer.from('534d4100000402A000000001000200000001', 'hex');
const tx = Buffer.from('534d4100000402A0FFFFFFFF0000002000000000', 'hex');

const networkErrors = ["ESOCKETTIMEDOUT", "ETIMEDOUT", "ECONNRESET", "ECONNREFUSED", "EHOSTUNREACH"];

const optionDefinitions = [
	{ name: 'mqtthost', alias: 'm', type: String, defaultValue: "localhost" },
	{ name: 'mqttclientid', alias: 'c', type: String, defaultValue: "SMAClient" },
	{ name: 'inverterhost', alias: 'i', type: String },
	{ name: 'inverterport', alias: 'p', type: String },
	{ name: 'address', alias: 'a', type: Number, multiple: true, defaultValue: [3] },
	{ name: 'wait', alias: 'w', type: Number, defaultValue: 10000 },
	{ name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
	{ name: 'scan', alias: 's', type: Boolean, defaultValue: false },
];

const options = commandLineArgs(optionDefinitions)

var SerialNumber = [];
var modbusClient = new ModbusRTU();
var mutex = new Mutex();


modbusClient.setTimeout(1000);

if (options.inverterhost) {
	modbusClient.connectTCP(options.inverterhost, { port: 502 }).then(val => {
		// start get value
		getStatus();
	}).catch((error) => {
		console.error("connectTcpRTUBuffered: " + error.message);
		process.exit(-1);
	});
} else if (options.inverterport) {
	modbusClient.connectRTUBuffered(options.inverterport, { baudRate: 9600, parity: 'none' }).then((val) => {
		// start get value
		getStatus();
	}).catch((error) => {
		console.error("connectRTUBuffered: " + error.message);
		process.exit(-1);
	});
}

console.log("MQTT Host         : " + options.mqtthost);
console.log("MQTT Client ID    : " + options.mqttclientid);

console.log("SMA MODBUS addr: " + options.address);

if (options.inverterhost) {
	console.log("SMA host       : " + options.inverterhost);
} else {
	console.log("SMA serial port: " + options.inverterport);
}

if (options.scan) {
	server.on('error', (err) => {
		console.error(`server error:\n${err.stack}`);
		server.close();
	});

	server.on('message', (msg, rinfo) => {
		if (msg.includes(rx)) {
			console.log("Found SMA product at " + rinfo.address);
		}
	});

	server.on('listening', () => {
		const address = server.address();
		console.log(`server listening ${address.address}:${address.port}`);
	});
	server.bind(9522, () => {
		server.setMulticastLoopback(false);
		server.addMembership('239.12.255.254');
		server.send(tx, 9522, '239.12.255.254', (err) => {
		});
	});
}

var MQTTclient = mqtt.connect("mqtt://" + options.mqtthost, { clientId: options.mqttclientid });
MQTTclient.on("connect", function () {
	console.log("MQTT connected");
	MQTTclient.subscribe("SMA/+/+/set");
})

MQTTclient.on("error", function (error) {
	console.log("Can't connect" + error);
	process.exit(1)
});

function sendMqtt(address, data) {
	if (options.debug) {
		console.log("publish: " + 'SMA/' + address, JSON.stringify(data));
	}
	MQTTclient.publish('SMA/' + address, JSON.stringify(data), { retain: true });
}

function findModbusAddr(serial) {
	var pos = 0;
	for (let address of options.address) {
		if (options.debug) {
			console.log("query: " + address);
		}
		if (SerialNumber[address] == serial) {
			if (options.debug) {
				console.log("found modbus address: ", address);
			}
			return address;
		}
		pos++;
	}
	if (options.debug) {
		console.log("modbus address not found for serial:", serial);
	}
	return -1;
}

async function modbusWrite(serial, func, reg, value, query = 0) {
	var addr = findModbusAddr(serial);
	if (addr > 0) {
		return await mutex.runExclusive(async () => {
			try {
				modbusClient.setID(addr);
				var ret;
				if (!query) {
					await modbusClient.writeRegister(reg, value);
					MQTTclient.publish('SMA/' + serial + "/" + func + "/result", value.toString());
				} else {
					ret = await modbusClient.readHoldingRegisters(reg, 1);
					MQTTclient.publish('SMA/' + serial + "/" + func + "/result", ret.buffer.readUInt16BE(0).toString());
				}
				return ret;
			} catch (e) {
				MQTTclient.publish('SMA/' + serial + "/" + func + "/result", "failed: " + e.message);
				console.error("modbusWrite: " + e.message);
			}
		});
	}
	return -1;
}

MQTTclient.on('message', function (topic, message, packet) {
	if (options.debug) {
		console.log("MQTT message for topic ", topic, " received: ", message);
	}
	if (topic.includes("SMA/")) {
		let sub = topic.split('/');
		let serial = sub[1];
		let func = sub[2];
		let value = parseInt(message);
		let query = message.length == 0
		let register = -1;
		// control functions ToDo...
		if (register != -1) {
			modbusWrite(serial, func, register, value, query);
		}
	}
});

async function getSN(address) {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(30005, 4);
		var SNStr = vals.buffer.readUInt32BE(0);
		SerialNumber[address] = SNStr
		if (options.debug) {
			console.log(SNStr);
		}
		return SNStr;
	} catch (e) {
		if (options.debug) {
			console.error("getSN: " + e.message);
		}
		if (e.errno) {
			if (networkErrors.includes(e.errno)) {
				process.exit(-1);
			}
		}
		return null;
	}
}

const PayloadParser_30513 = new Parser()
	.uint64be('TotalPVGeneration', { formatter: (x) => { return Number(x) / 1000.0 } })
	.uint64be('TodayPVGeneration', { formatter: (x) => { return Number(x) / 1000.0 } })
	;

const PayloadParser_30769 = new Parser()
	.int32be('PV1Current', { formatter: (x) => { return x / 1000.0; } })
	.int32be('PV1Voltage', { formatter: (x) => { return x / 100.0; } })
	.uint32be('PV1Power')
	.int32be('ActivePower')
	.int32be('L1ActivePower')
	.int32be('L2ActivePower')
	.int32be('L3ActivePower')
	.uint32be('L1Voltage', { formatter: (x) => { return x / 100.0; } })
	.uint32be('L2Voltage', { formatter: (x) => { return x / 100.0; } })
	.uint32be('L3Voltage', { formatter: (x) => { return x / 100.0; } })
	.seek(12)
	//	.uint32be('L1L2Voltage', { formatter: (x) => { return x / 100.0; } })
	//	.uint32be('L2L3Voltage', { formatter: (x) => { return x / 100.0; } })
	//	.uint32be('L3L1Voltage', { formatter: (x) => { return x / 100.0; } })
	.uint32be('TotalCurrent', { formatter: (x) => { return x / 1000.0; } })
	.seek(12)
	//	.uint32be('L1Current', { formatter: (x) => { return x / 1000.0; } })
	//	.uint32be('L2Current', { formatter: (x) => { return x / 1000.0; } })
	//	.uint32be('L3Current', { formatter: (x) => { return x / 1000.0; } })
	.uint32be('Frequency', { formatter: (x) => { return x / 100.0; } })
	.int32be('ReactivePower')
	.int32be('L1ReactivePower')
	.int32be('L2ReactivePower')
	.int32be('L3ReactivePower')
	.int32be('ApparentPower')
	.int32be('L1ApparentPower')
	.int32be('L2ApparentPower')
	.int32be('L3ApparentPower')
	//	.uint32be('cosphi', { formatter: (x) => { return x / 100.0; } })
	//	.uint32be('cosphimode')
	;

const PayloadParser_30953 = new Parser()
	.int32be('Temperature', { formatter: (x) => { return x / 10.0; } })
	.seek(4)
	.int32be('PV2Current', { formatter: (x) => { return x / 1000.0; } })
	.int32be('PV2Voltage', { formatter: (x) => { return x / 100.0; } })
	.uint32be('PV2Power')
	;

const getRegisters = async (address) => {
	try {
		modbusClient.setID(address);
		let vals = await modbusClient.readHoldingRegisters(30513, 8);
		var state_30513 = PayloadParser_30513.parse(vals.buffer);
		vals = await modbusClient.readHoldingRegisters(30769, 76);
		var state_30769 = PayloadParser_30769.parse(vals.buffer);
		vals = await modbusClient.readHoldingRegisters(30953, 14);
		var state_30953 = PayloadParser_30953.parse(vals.buffer);
		if (state_30953.PV2Power != 0x80000000) {
			var fullState = {};
			Object.assign(fullState, state_30513, state_30769, state_30953);
			await sendMqtt(SerialNumber[address], fullState);
		}

		if (options.debug) {
			console.log(util.inspect(fullState));
		}
		return fullState;
	} catch (e) {
		if (options.debug) {
			console.error("getRegisters: " + e.message);
		}
		if (e.errno) {
			if (networkErrors.includes(e.errno)) {
				process.exit(-1);
			}
		}
		return null;
	}
}


const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function getStatus() {
	try {
		var pos = 0;
		// get value of all addresss
		for (let address of options.address) {
			if (options.debug) {
				console.log("query: " + address);
			}
			await mutex.runExclusive(async () => {
				if (!SerialNumber[address]) {
					await getSN(address);
				}
			});
			await sleep(100);
			await mutex.runExclusive(async () => {
				if (SerialNumber[address]) {
					await getRegisters(address);
				}
			});
			pos++;
		}
		await sleep(options.wait);
	} catch (e) {
		// if error, handle them here (it should not)
		console.error("getStatus: " + e.message)
	} finally {
		// after get all data from salve repeate it again
		setImmediate(() => {
			getStatus();
		})
	}
}


