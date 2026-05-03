const kafka = require('../config/kafka');
const fs = require('fs');
const path = require('path');

const logFilePath = path.join(__dirname, '..', '..', 'location-log.txt');

const createDbWriterConsumer = () => {
  const consumer = kafka.consumer({ groupId: 'db-writer' });

  const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'location-updates', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const locationData = JSON.parse(message.value.toString());
        const logMessage = `DBWriter received: ${JSON.stringify(locationData)}\n`;
        
        // In a real app, you would write to a database here.
        // For this example, we append to a log file.
        fs.appendFile(logFilePath, logMessage, (err) => {
          if (err) {
            console.error('Failed to write to log file', err);
          }
        });
        console.log(logMessage.trim());
      },
    });
  };

  const shutdown = async () => {
    await consumer.disconnect();
  };

  return { run, shutdown };
};

module.exports = { createDbWriterConsumer };
