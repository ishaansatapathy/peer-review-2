const kafka = require('../config/kafka');

const createBroadcasterConsumer = (io) => {
  const consumer = kafka.consumer({ groupId: 'socket-broadcaster' });

  const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'location-updates', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const locationData = JSON.parse(message.value.toString());
        console.log(`Broadcaster received: ${JSON.stringify(locationData)}`);

        // Emit to a room named after the userId
        // This assumes clients join a room with their userId on connection
        if (locationData.userId) {
          io.to(locationData.userId).emit('locationUpdate', locationData);
        }
      },
    });
  };

  const shutdown = async () => {
    await consumer.disconnect();
  };

  return { run, shutdown };
};

module.exports = { createBroadcasterConsumer };
