const kafka = require('../config/kafka');

const producer = kafka.producer();
const topic = 'location-updates';

const connectProducer = async () => {
  await producer.connect();
  console.log('Kafka producer connected');
};

const disconnectProducer = async () => {
  await producer.disconnect();
  console.log('Kafka producer disconnected');
};

const publishLocation = async (locationData) => {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(locationData) }],
    });
  } catch (error) {
    console.error('Failed to publish location to Kafka', error);
  }
};

module.exports = {
  connectProducer,
  disconnectProducer,
  publishLocation,
};
