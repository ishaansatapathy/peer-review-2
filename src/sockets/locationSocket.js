const jwt = require('jsonwebtoken');
const { publishLocation } = require('../services/kafkaService');

const initializeSocket = (io) => {
  io.use((socket, next) => {
    const token = socket.handshake.auth.token;
    if (!token) {
      return next(new Error('Authentication error'));
    }
    jwt.verify(token, process.env.JWT_SECRET, (err, decoded) => {
      if (err) {
        return next(new Error('Authentication error'));
      }
      socket.userId = decoded.userId;
      next();
    });
  });

  io.on('connection', (socket) => {
    console.log(`Socket connected: ${socket.id} for user: ${socket.userId}`);

    // Join a room based on the userId
    socket.join(socket.userId);

    socket.on('locationUpdate', (location) => {
      if (!location || typeof location.latitude !== 'number' || typeof location.longitude !== 'number') {
        socket.emit('error', 'Invalid location data');
        return;
      }

      const locationData = {
        userId: socket.userId,
        latitude: location.latitude,
        longitude: location.longitude,
        timestamp: new Date().toISOString(),
      };

      publishLocation(locationData);
    });

    socket.on('disconnect', () => {
      console.log(`Socket disconnected: ${socket.id}`);
    });
  });
};

module.exports = { initializeSocket };
