require('dotenv').config();
const { app, server, io } = require('./config/express');
const { connectProducer, disconnectProducer } = require('./services/kafkaService');
const { initializeSocket } = require('./sockets/locationSocket');
const { createBroadcasterConsumer } = require('./services/socketBroadcaster');
const { createDbWriterConsumer } = require('./services/dbWriter');
const authRoutes = require('./routes/auth');
const express = require('express');
const path = require('path');

// Serve static files
app.use(express.static(path.join(__dirname, '..', 'public')));

// Setup routes
app.use('/auth', authRoutes);

// Initialize Socket.IO
initializeSocket(io);

const startServer = async () => {
  await connectProducer();

  // Create and run consumers
  const broadcasterConsumer = createBroadcasterConsumer(io);
  const dbWriterConsumer = createDbWriterConsumer();
  
  await broadcasterConsumer.run();
  await dbWriterConsumer.run();

  const port = process.env.PORT || 3000;
  server.listen(port, () => {
    console.log(`Server listening on port ${port}`);
  });

  const gracefulShutdown = async () => {
    console.log('Shutting down gracefully...');
    await disconnectProducer();
    await broadcasterConsumer.shutdown();
    await dbWriterConsumer.shutdown();
    server.close(() => {
      console.log('Server closed.');
      process.exit(0);
    });
  };

  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);
};

module.exports = { startServer };
