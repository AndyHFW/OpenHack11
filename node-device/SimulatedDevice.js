// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

'use strict';

// The device connection string to authenticate the device with your IoT hub.
//
// NOTE:
// For simplicity, this sample sets the connection string in code.
// In a production environment, the recommended approach is to use
// an environment variable to make it available to your application
// or use an HSM or an x509 certificate.
// https://docs.microsoft.com/azure/iot-hub/iot-hub-devguide-security
//
// Using the Azure CLI:
// az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyNodeDevice --output table
var connectionString = 'HostName=TorontoOpenHack11.azure-devices.net;DeviceId=Hacker2Device;SharedAccessKey=SSxzMYzKnuCtCBkkpWvtj4ZcAqG7YkoUTrW9Cx4gFIM=';

// Using the Node.js Device SDK for IoT Hub:
//   https://github.com/Azure/azure-iot-sdk-node
// The sample connects to a device-specific MQTT endpoint on your IoT Hub.
var Mqtt = require('azure-iot-device-mqtt').Mqtt;
var DeviceClient = require('azure-iot-device').Client
var Message = require('azure-iot-device').Message;
const uuidv1 = require('uuid/v1');

var client = DeviceClient.fromConnectionString(connectionString, Mqtt);

// Create a message and send it to the IoT hub every second
setInterval(function(){
  // Simulate telemetry.
  var ticketId = uuidv1();
  var message = new Message(JSON.stringify({
    ticketId: ticketId,
    entryTime: new Date().toISOString()
  }));

  // Add a custom application property to the message.
  // An IoT hub can filter on these properties without access to the message body.
  // message.properties.add('temperatureAlert', (temperature > 30) ? 'true' : 'false');

  console.log('Sending message: ' + message.getData());

  // Send the message.
  client.sendEvent(message, function (err) {
    if (err) {
      console.error('send error: ' + err.toString());
    } else {
      console.log('message sent');
    }
  });
}, 1000);
